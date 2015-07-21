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
package org.apache.hadoop.yarn.server.timelineservice.storage;


import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.storage.app2flow.App2FlowColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.app2flow.App2FlowRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.app2flow.App2FlowTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

public class HBaseTimelineReaderImpl
    extends AbstractService implements TimelineReader  {

  private static final Log LOG = LogFactory
      .getLog(HBaseTimelineReaderImpl.class);

  private Configuration hbaseConf = null;
  private Connection conn;
  private EntityTable entityTable;
  private App2FlowTable app2FlowTable;

  public HBaseTimelineReaderImpl() {
    super(HBaseTimelineReaderImpl.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    hbaseConf = HBaseConfiguration.create(conf);
    conn = ConnectionFactory.createConnection(hbaseConf);
    entityTable = new EntityTable();
    app2FlowTable = new App2FlowTable();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (conn != null) {
      LOG.info("closing the hbase Connection");
      conn.close();
    }
    super.serviceStop();
  }

  @Override
  public TimelineEntity getEntity(String userId, String clusterId,
      String flowId, Long flowRunId, String appId, String entityType,
      String entityId, EnumSet<Field> fieldsToRetrieve)
      throws IOException {
    validateParams(userId, clusterId, appId, entityType, entityId, true);
    // In reality both should be null or neither should be null
    if (flowId == null || flowRunId == null) {
      FlowContext context = lookupFlowContext(clusterId, appId);
      flowId = context.flowId;
      flowRunId = context.flowRunId;
    }
    if (fieldsToRetrieve == null) {
      fieldsToRetrieve = EnumSet.noneOf(Field.class);
    }

    byte[] rowKey = EntityRowKey.getRowKey(
        clusterId, userId, flowId, flowRunId, appId, entityType, entityId);
    Get get = new Get(rowKey);
    return getEntity(
        entityTable.getResult(hbaseConf, conn, get), fieldsToRetrieve,
        false, 0L, 0L, false, 0L, 0L, null, null, null, null, null, null);
  }

  @Override
  public Set<TimelineEntity> getEntities(String userId, String clusterId,
      String flowId, Long flowRunId, String appId, String entityType,
      Long limit, Long createdTimeBegin, Long createdTimeEnd,
      Long modifiedTimeBegin, Long modifiedTimeEnd,
      Map<String, Set<String>> relatesTo, Map<String, Set<String>> isRelatedTo,
      Map<String, Object> infoFilters, Map<String, String> configFilters,
      Set<String> metricFilters, Set<String> eventFilters,
      EnumSet<Field> fieldsToRetrieve) throws IOException {
    validateParams(userId, clusterId, appId, entityType, null, false);
    // In reality both should be null or neither should be null
    if (flowId == null || flowRunId == null) {
      FlowContext context = lookupFlowContext(clusterId, appId);
      flowId = context.flowId;
      flowRunId = context.flowRunId;
    }
    if (limit == null) {
      limit = TimelineReader.DEFAULT_LIMIT;
    }
    if (createdTimeBegin == null) {
      createdTimeBegin = 0L;
    }
    if (createdTimeEnd == null) {
      createdTimeEnd = Long.MAX_VALUE;
    }
    if (modifiedTimeBegin == null) {
      modifiedTimeBegin = 0L;
    }
    if (modifiedTimeEnd == null) {
      modifiedTimeEnd = Long.MAX_VALUE;
    }
    if (fieldsToRetrieve == null) {
      fieldsToRetrieve = EnumSet.noneOf(Field.class);
    }

    NavigableSet<TimelineEntity> entities = new TreeSet<>();
    // Scan through part of the table to find the entities belong to one app and
    // one type
    Scan scan = new Scan();
    scan.setRowPrefixFilter(EntityRowKey.getRowKeyPrefix(
        clusterId, userId, flowId, flowRunId, appId, entityType));
    scan.setMaxVersions(Integer.MAX_VALUE);
    ResultScanner scanner = entityTable.getResultScanner(hbaseConf, conn, scan);
    for (Result result : scanner) {
      byte[] row = result.getRow();
      TimelineEntity entity = getEntity(result, fieldsToRetrieve,
          true, createdTimeBegin, createdTimeEnd,
          true, modifiedTimeBegin, modifiedTimeEnd,
          isRelatedTo, relatesTo, infoFilters, configFilters, eventFilters,
          metricFilters);
      if (entity == null) {
        continue;
      }
      if (entities.size() > limit) {
        entities.pollLast();
      }
      entities.add(entity);
    }
    return entities;
  }

  private FlowContext lookupFlowContext(String clusterId, String appId)
      throws IOException {
    byte[] rowKey = App2FlowRowKey.getRowKey(clusterId, appId);
    Get get = new Get(rowKey);
    Result result = app2FlowTable.getResult(hbaseConf, conn, get);
    if (result != null) {
      return new FlowContext(
          App2FlowColumn.FLOW_ID.readResult(result).toString(),
          ((Number) App2FlowColumn.FLOW_RUN_ID.readResult(result)).longValue());
    } else {
       throw new IOException(
           "Unable to find the context flow ID and flow run ID for clusterId=" +
           clusterId + ", appId=" + appId);
    }
  }

  private static class FlowContext {
    private String flowId;
    private Long flowRunId;
    public FlowContext(String flowId, Long flowRunId) {
      this.flowId = flowId;
      this.flowRunId = flowRunId;
    }
  }

  private static void validateParams(String userId, String clusterId,
      String appId, String entityType, String entityId, boolean checkEntityId) {
    Preconditions.checkNotNull(userId, "userId shouldn't be null");
    Preconditions.checkNotNull(clusterId, "clusterId shouldn't be null");
    Preconditions.checkNotNull(appId, "appId shouldn't be null");
    Preconditions.checkNotNull(entityType, "entityType shouldn't be null");
    if (checkEntityId) {
      Preconditions.checkNotNull(entityId, "entityId shouldn't be null");
    }
  }

  private static TimelineEntity getEntity(
      Result result, EnumSet<Field> fieldsToRetrieve,
      boolean checkCreatedTime, long createdTimeBegin, long createdTimeEnd,
      boolean checkModifiedTime, long modifiedTimeBegin, long modifiedTimeEnd,
      Map<String, Set<String>> isRelatedTo, Map<String, Set<String>> relatesTo,
      Map<String, Object> infoFilters, Map<String, String> configFilters,
      Set<String> eventFilters, Set<String> metricFilters)
          throws IOException {
    if (result == null || result.isEmpty()) {
      return null;
    }
    TimelineEntity entity = new TimelineEntity();
    entity.setType(EntityColumn.TYPE.readResult(result).toString());
    entity.setId(EntityColumn.ID.readResult(result).toString());

    // fetch created time
    entity.setCreatedTime(
        ((Number) EntityColumn.CREATED_TIME.readResult(result)).longValue());
    if (checkCreatedTime && (entity.getCreatedTime() < createdTimeBegin ||
        entity.getCreatedTime() > createdTimeEnd)) {
      return null;
    }

    // fetch modified time
    entity.setCreatedTime(
        ((Number) EntityColumn.MODIFIED_TIME.readResult(result)).longValue());
    if (checkModifiedTime && (entity.getModifiedTime() < modifiedTimeBegin ||
        entity.getModifiedTime() > modifiedTimeEnd)) {
      return null;
    }

    // fetch is related to entities
    boolean checkIsRelatedTo = isRelatedTo != null && isRelatedTo.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.IS_RELATED_TO) || checkIsRelatedTo) {
      readConnections(entity, result, EntityColumnPrefix.IS_RELATED_TO);
      if (checkIsRelatedTo && !TimelineReaderUtils.matchRelations(
          entity.getIsRelatedToEntities(), isRelatedTo)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.IS_RELATED_TO)) {
        entity.getIsRelatedToEntities().clear();
      }
    }

    // fetch relates to entities
    boolean checkRelatesTo = relatesTo != null && relatesTo.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.RELATES_TO) || checkRelatesTo) {
      readConnections(entity, result, EntityColumnPrefix.RELATES_TO);
      if (checkRelatesTo && !TimelineReaderUtils.matchRelations(
          entity.getRelatesToEntities(), relatesTo)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.RELATES_TO)) {
        entity.getRelatesToEntities().clear();
      }
    }

    // fetch info
    boolean checkInfo = infoFilters != null && infoFilters.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.INFO) || checkInfo) {
      // TODO
      if (checkInfo &&
          !TimelineReaderUtils.matchFilters(entity.getInfo(), infoFilters)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.INFO)) {
        entity.getInfo().clear();
      }
    }

    // fetch configs
    boolean checkConfigs = configFilters != null && configFilters.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.CONFIGS) || checkConfigs) {
      readKeyValuePairs(entity, result, EntityColumnPrefix.CONFIG);
      if (checkConfigs && !TimelineReaderUtils.matchFilters(
          entity.getConfigs(), configFilters)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.CONFIGS)) {
        entity.getConfigs().clear();
      }
    }

    // fetch events
    boolean checkEvents = eventFilters != null && eventFilters.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.EVENTS) || checkEvents) {
      readEvents(entity, result);
      if (checkEvents && !TimelineReaderUtils.matchEventFilters(
          entity.getEvents(), eventFilters)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.EVENTS)) {
        entity.getEvents().clear();
      }
    }

    // fetch metrics
    boolean checkMetrics = metricFilters != null && metricFilters.size() > 0;
    if (fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(Field.METRICS) || checkMetrics) {
      readMetrics(entity, result);
      if (checkMetrics && !TimelineReaderUtils.matchMetricFilters(
          entity.getMetrics(), metricFilters)) {
        return null;
      }
      if (!fieldsToRetrieve.contains(Field.ALL) &&
          !fieldsToRetrieve.contains(Field.METRICS)) {
        entity.getMetrics().clear();
      }
    }
    return entity;
  }

  private static void readConnections(
      TimelineEntity entity, Result result, EntityColumnPrefix prefix)
          throws IOException {
    // isRelatedTo and relatesTo are of type Map<String, Set<String>>
    Map<String, Object> columns = prefix.readResults(result);
    for (Map.Entry<String, Object> column : columns.entrySet()) {
      for (String id : Separator.VALUES.splitEncoded(
          column.getValue().toString())) {
        if (prefix.equals(EntityColumnPrefix.IS_RELATED_TO)) {
          entity.addIsRelatedToEntity(column.getKey(), id);
        } else {
          entity.addRelatesToEntity(column.getKey(), id);
        }
      }
    }
  }

  private static void readKeyValuePairs(
      TimelineEntity entity, Result result, EntityColumnPrefix prefix)
          throws IOException {
    // info and configuration are of type Map<String, Object or String>
    Map<String, Object> columns = prefix.readResults(result);
    if (prefix.equals(EntityColumnPrefix.CONFIG)) {
      for (Map.Entry<String, Object> column : columns.entrySet()) {
        entity.addConfig(column.getKey(), column.getKey().toString());
      }
    } else {
      entity.addInfo(columns);
    }
  }

  private static void readEvents(TimelineEntity entity, Result result)
      throws IOException {
    // TODO
  }

  private static void readMetrics(TimelineEntity entity, Result result)
      throws IOException {
    NavigableMap<String, NavigableMap<Long, Number>> metricsResult =
        EntityColumnPrefix.METRIC.readTimeseriesResults(result);
    for (Map.Entry<String, NavigableMap<Long, Number>> metricResult:
        metricsResult.entrySet()) {
      TimelineMetric metric = new TimelineMetric();
      metric.setId(metricResult.getKey());
      // Simply assume that if the value set contains more than 1 elements, the
      // metric is a TIME_SERIES metric, otherwise, it's a SINGLE_VALUE metric
      metric.setType(metricResult.getValue().size() > 1 ?
          TimelineMetric.Type.TIME_SERIES : TimelineMetric.Type.SINGLE_VALUE);
      metric.addValues(metricResult.getValue());
      entity.addMetric(metric);
    }
  }
}
