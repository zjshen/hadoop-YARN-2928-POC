/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.aggregation.timebased;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Private
@Unstable
public class PhoenixAggregatorStorage extends AbstractService {

  public static final String TIMELINE_SERVICE_PHOENIX_STORAGE_CONN_STR
      = YarnConfiguration.TIMELINE_SERVICE_PREFIX
          + "writer.phoenix.connectionString";

  public static final String TIMELINE_SERVICE_PHEONIX_STORAGE_CONN_STR_DEFAULT
      = "jdbc:phoenix:localhost:2181:/hbase";

  private static final Log LOG
      = LogFactory.getLog(PhoenixAggregatorStorage.class);
  private static final String PHOENIX_COL_FAMILY_PLACE_HOLDER
      = "timeline_cf_placeholder";

  /** Default Phoenix JDBC driver name */
  private static final String DRIVER_CLASS_NAME
      = "org.apache.phoenix.jdbc.PhoenixDriver";

  /** Default Phoenix timeline config column family */
  private static final String METRIC_COLUMN_FAMILY = "m.";
  /** Default Phoenix timeline info column family */
  private static final String INFO_COLUMN_FAMILY = "i.";
  /** Default separator for Phoenix storage */
  private static final String AGGREGATION_STORAGE_SEPARATOR = ";";

  /** Connection string to the deployed Phoenix cluster */
  @VisibleForTesting
  String connString = null;
  @VisibleForTesting
  Properties connProperties = new Properties();

  PhoenixAggregatorStorage() {
    super((PhoenixAggregatorStorage.class.getName()));
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // so check it here and only read in the config if it's not overridden.
    connString =
        conf.get(TIMELINE_SERVICE_PHOENIX_STORAGE_CONN_STR,
        TIMELINE_SERVICE_PHEONIX_STORAGE_CONN_STR_DEFAULT);
    createTables();
    super.init(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  public TimelineWriteResponse writeFlowAggregation(String clusterId,
      String userId, String flowName, TimelineEntities entities)
      throws IOException {
    return writeAggregatedEntity(clusterId, userId, flowName, entities,
          AggregationStorageInfo.FLOW_AGGREGATION);
  }

  public TimelineWriteResponse writeUserAggregation(String clusterId,
      String userId, TimelineEntities entities)
      throws IOException {
    return writeAggregatedEntity(clusterId, userId, null, entities,
        AggregationStorageInfo.USER_AGGREGATION);
  }

  // TODO: reuse the code for userAggregatedEntity, using AggregationStorageInfo
  public TimelineEntity readFlowAggregatedEntity(String clusterId,
      String userId, String flowName) throws IOException {
    TimelineCollectorContext context = new TimelineCollectorContext();
    context.setClusterId(clusterId);
    context.setUserId(userId);
    context.setFlowName(flowName);

    String whereClause = " WHERE user=? AND cluster=? AND flow_name=?";
    String sql = "SELECT created_time, modified_time, metric_names FROM "
        + AggregationStorageInfo.FLOW_AGGREGATION.getTableName() + whereClause;
    TimelineEntity entity = new TimelineEntity();
    String metrics;
    try (PreparedStatement psStmt = getConnection().prepareStatement(sql)) {
      AggregationStorageInfo.FLOW_AGGREGATION.setStringsForPrimaryKey(psStmt,
          context, null, 1);
      try (ResultSet rs = psStmt.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        entity.setCreatedTime(rs.getLong(1));
        entity.setModifiedTime(rs.getLong(2));
        metrics = rs.getString(3);
      }
    } catch (SQLException se) {
      LOG.error("Failed to add entity to Phoenix " + se.getMessage());
      throw new IOException(se);
    } catch (Exception e) {
      LOG.error("Exception on getting connection: " + e.getMessage());
      throw new IOException(e);
    }

    String[] rawMetricNames = metrics.split(AGGREGATION_STORAGE_SEPARATOR);
    String[] metricNamesWithPrefix = new String[rawMetricNames.length];
    for (int index = 0; index < rawMetricNames.length; index++) {
      metricNamesWithPrefix[index] = METRIC_COLUMN_FAMILY
          + rawMetricNames[index];
    }
    Set<String> metricNames
        = new HashSet<>(Arrays.asList(metricNamesWithPrefix));
    if (metricNames.size() > 0) {

      StringBuilder metricSQL = new StringBuilder("SELECT ").append(
          StringUtils.join(AggregationStorageInfo.FLOW_AGGREGATION
              .getPrimaryKeyList(), ",")).append(",").append(
          StringUtils.join(metricNames, ",")).append(" FROM ")
          .append(AggregationStorageInfo.FLOW_AGGREGATION.getTableName())
          .append("(");
      // FIXME: set the column family to empty string to avoid setting m. twice.
      appendColumnsSQL(metricSQL, new DynamicColumns<>(
          "", DynamicColumns.COLUMN_FAMILY_TYPE_BYTES,
          metricNames));
      metricSQL.append(") ").append(whereClause);

      try (PreparedStatement metricPreparedStmt
          = getConnection().prepareStatement(metricSQL.toString())) {
        AggregationStorageInfo.FLOW_AGGREGATION.setStringsForPrimaryKey(
            metricPreparedStmt, context, null, 1);
        try (ResultSet metricRS
            = metricPreparedStmt.executeQuery()) {
          if (!metricRS.next()) {
            return entity;
          }
          ResultSetMetaData metricRsMetadata = metricRS.getMetaData();
          int initialMetricIdx = AggregationStorageInfo.FLOW_AGGREGATION
              .getPrimaryKeyList().length + 1;
          for (int i = initialMetricIdx; i <= metricRsMetadata.getColumnCount();
               i++) {
            byte[] rawData = metricRS.getBytes(i);
            ObjectMapper metricMapper = new ObjectMapper();
            TimelineMetric metric =
                metricMapper.readValue(rawData, TimelineMetric.class);
            entity.addMetric(metric);
          }
        }
      } catch (SQLException se) {
        LOG.error("Failed to add entity to Phoenix " + se.getMessage());
        throw new IOException(se);
      } catch (Exception e) {
        LOG.error("Exception on getting connection: " + e.getMessage());
        throw new IOException(e);
      }
    }
    return entity;
  }

  @Private
  @VisibleForTesting
  TimelineWriteResponse writeAggregatedEntity(String clusterId,
      String userId, String flowName, TimelineEntities entities,
      AggregationStorageInfo aggregationInfo) throws IOException {
    TimelineWriteResponse response = new TimelineWriteResponse();
    TimelineCollectorContext currContext = new TimelineCollectorContext(
        clusterId, userId, flowName, null, 0, null);
    String sql = "UPSERT INTO " + aggregationInfo.getTableName()
        + " (" + StringUtils.join(aggregationInfo.getPrimaryKeyList(), ",")
        + ", created_time, modified_time, metric_names, info_keys) "
        + "VALUES ("
        + StringUtils.repeat("?,", aggregationInfo.getPrimaryKeyList().length)
        + "?, ?, ?, ?)";
    if (LOG.isDebugEnabled()) {
      LOG.debug("TimelineEntity write SQL: " + sql);
    }

    try (Connection conn = getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      for (TimelineEntity entity : entities.getEntities()) {
        HashMap<String, TimelineMetric> formattedMetrics = null;
        if (entity.getMetrics() != null) {
          formattedMetrics = new HashMap<>();
          for (TimelineMetric m : entity.getMetrics()) {
            formattedMetrics.put(m.getId(), m);
          }
        }
        int idx = aggregationInfo.setStringsForPrimaryKey(ps, currContext, null,
            1);
        ps.setLong(idx++, entity.getCreatedTime());
        ps.setLong(idx++, entity.getModifiedTime());
        ps.setString(idx++, StringUtils.join(formattedMetrics.keySet().toArray(),
            AGGREGATION_STORAGE_SEPARATOR));
        ps.setString(idx++, StringUtils.join(entity.getInfo().keySet().toArray(),
            AGGREGATION_STORAGE_SEPARATOR));
        ps.execute();

        storeEntityVariableLengthFields(entity, formattedMetrics, currContext,
            conn, aggregationInfo);

        conn.commit();
      }
    } catch (SQLException se) {
      LOG.error("Failed to add entity to Phoenix " + se.getMessage());
      throw new IOException(se);
    } catch (Exception e) {
      LOG.error("Exception on getting connection: " + e.getMessage());
      throw new IOException(e);
    }
    return response;
  }

  // Utility functions
  @Private
  @VisibleForTesting
  Connection getConnection() throws IOException {
    Connection conn;
    try {
      Class.forName(DRIVER_CLASS_NAME);
      conn = DriverManager.getConnection(connString, connProperties);
      conn.setAutoCommit(false);
    } catch (SQLException se) {
      LOG.error("Failed to connect to phoenix server! "
          + se.getLocalizedMessage());
      throw new IOException(se);
    } catch (ClassNotFoundException e) {
      LOG.error("Class not found! " + e.getLocalizedMessage());
      throw new IOException(e);
    }
    return conn;
  }

  private void createTables() throws Exception {
    // Create tables if necessary
    try (Connection conn = getConnection();
        Statement stmt = conn.createStatement()) {
      // Table schema defined as in YARN-3817.
      String sql = "CREATE TABLE IF NOT EXISTS "
          + AggregationStorageInfo.FLOW_AGGREGATION_TABLE_NAME
          + "(user VARCHAR NOT NULL, cluster VARCHAR NOT NULL, "
          + "flow_name VARCHAR NOT NULL, "
          + "created_time UNSIGNED_LONG, modified_time UNSIGNED_LONG, "
          + INFO_COLUMN_FAMILY + PHOENIX_COL_FAMILY_PLACE_HOLDER + " VARBINARY, "
          + METRIC_COLUMN_FAMILY + PHOENIX_COL_FAMILY_PLACE_HOLDER + " VARBINARY, "
          + "metric_names VARCHAR, info_keys VARCHAR "
          + "CONSTRAINT pk PRIMARY KEY("
          + "user, cluster, flow_name))";
      stmt.executeUpdate(sql);
      sql = "CREATE TABLE IF NOT EXISTS "
          + AggregationStorageInfo.USER_AGGREGATION_TABLE_NAME
          + "(user VARCHAR NOT NULL, "
          + "created_time UNSIGNED_LONG, modified_time UNSIGNED_LONG, "
          + INFO_COLUMN_FAMILY + PHOENIX_COL_FAMILY_PLACE_HOLDER + " VARBINARY, "
          + METRIC_COLUMN_FAMILY + PHOENIX_COL_FAMILY_PLACE_HOLDER + " VARBINARY, "
          + "metric_names VARCHAR, info_keys VARCHAR "
          + "CONSTRAINT pk PRIMARY KEY(user))";
      stmt.executeUpdate(sql);
      conn.commit();
    } catch (SQLException se) {
      LOG.error("Failed in init data " + se.getLocalizedMessage());
      throw se;
    }
    return;
  }

  private static class DynamicColumns<K> {
    static final String COLUMN_FAMILY_TYPE_BYTES = " VARBINARY";
    static final String COLUMN_FAMILY_TYPE_STRING = " VARCHAR";
    String columnFamilyPrefix;
    String type;
    Set<K> columns;

    public DynamicColumns(String columnFamilyPrefix, String type,
        Set<K> keyValues) {
      this.columnFamilyPrefix = columnFamilyPrefix;
      this.columns = keyValues;
      this.type = type;
    }
  }

  private static <K> StringBuilder appendColumnsSQL(
      StringBuilder colNames, DynamicColumns<K> cfInfo) {
    boolean first = true;
    // Prepare the sql template by iterating through all keys
    for (K key : cfInfo.columns) {
      if (!first) {
        colNames.append(",");
      } else {
        first = false;
      }
      colNames.append(cfInfo.columnFamilyPrefix)
          .append(key.toString()).append(cfInfo.type);
    }
    return colNames;
  }

  private static <K, V> int setValuesForColumnFamily(
      PreparedStatement ps, Map<K, V> keyValues, int startPos,
      boolean converToBytes) throws SQLException {
    int idx = startPos;
    for (Map.Entry<K, V> entry : keyValues.entrySet()) {
      V value = entry.getValue();
      if (value instanceof Collection) {
        ps.setString(idx++, StringUtils.join(
            (Collection) value, AGGREGATION_STORAGE_SEPARATOR));
      } else {
        if (converToBytes) {
          try {
            ps.setBytes(idx++, GenericObjectMapper.write(value));
          } catch (IOException ie) {
            LOG.error("Exception in converting values into bytes "
                + ie.getMessage());
            throw new SQLException(ie);
          }
        } else {
          ps.setString(idx++, value.toString());
        }
      }
    }
    return idx;
  }

  private static <K, V> int setBytesForColumnFamily(
      PreparedStatement ps, Map<K, V> keyValues, int startPos)
      throws SQLException {
    return setValuesForColumnFamily(ps, keyValues, startPos, true);
  }

  private static <K, V> int setStringsForColumnFamily(
      PreparedStatement ps, Map<K, V> keyValues, int startPos)
      throws SQLException {
    return setValuesForColumnFamily(ps, keyValues, startPos, false);
  }

  private static void storeEntityVariableLengthFields(TimelineEntity entity,
      Map<String, TimelineMetric> formattedMetrics,
      TimelineCollectorContext context, Connection conn,
      AggregationStorageInfo aggregationInfo) throws SQLException {
    int numPlaceholders = 0;
    StringBuilder columnDefs = new StringBuilder(
        StringUtils.join(aggregationInfo.getPrimaryKeyList(), ","));
    if (entity.getInfo() != null) {
      Set<String> keySet = entity.getInfo().keySet();
      columnDefs.append(",");
      appendColumnsSQL(columnDefs, new DynamicColumns<>(
          INFO_COLUMN_FAMILY, DynamicColumns.COLUMN_FAMILY_TYPE_BYTES,
          keySet));
      numPlaceholders += keySet.size();
    }
    if (formattedMetrics != null) {
      columnDefs.append(",");
      appendColumnsSQL(columnDefs, new DynamicColumns<>(
          METRIC_COLUMN_FAMILY, DynamicColumns.COLUMN_FAMILY_TYPE_BYTES,
          formattedMetrics.keySet()));
      numPlaceholders += formattedMetrics.keySet().size();
    }
    if (numPlaceholders == 0) {
      return;
    }
    StringBuilder placeholders = new StringBuilder();
    placeholders.append(
        StringUtils.repeat("?,", aggregationInfo.getPrimaryKeyList().length));
    // numPlaceholders >= 1 now
    placeholders.append("?")
        .append(StringUtils.repeat(",?", numPlaceholders - 1));
    String sqlVariableLengthFields = new StringBuilder("UPSERT INTO ")
        .append(aggregationInfo.getTableName()).append(" (").append(columnDefs)
        .append(") VALUES(").append(placeholders).append(")").toString();
    if (LOG.isDebugEnabled()) {
      LOG.debug("SQL statement for variable length fields: "
          + sqlVariableLengthFields);
    }
    // Use try with resource statement for the prepared statement
    try (PreparedStatement psVariableLengthFields =
        conn.prepareStatement(sqlVariableLengthFields)) {
      int idx = aggregationInfo.setStringsForPrimaryKey(
          psVariableLengthFields, context, null, 1);
      if (entity.getInfo() != null) {
        idx = setBytesForColumnFamily(
            psVariableLengthFields, entity.getInfo(), idx);
      }
      if (formattedMetrics != null) {
        idx = setBytesForColumnFamily(
            psVariableLengthFields, formattedMetrics, idx);
      }
      psVariableLengthFields.execute();
    }
  }

  // WARNING: This method will permanently drop a table!
  @Private
  @VisibleForTesting
  void dropTable(String tableName) throws Exception {
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      String sql = "DROP TABLE " + tableName;
      stmt.executeUpdate(sql);
    } catch (SQLException se) {
      LOG.error("Failed in dropping entity table " + se.getLocalizedMessage());
      throw se;
    }
  }
}
