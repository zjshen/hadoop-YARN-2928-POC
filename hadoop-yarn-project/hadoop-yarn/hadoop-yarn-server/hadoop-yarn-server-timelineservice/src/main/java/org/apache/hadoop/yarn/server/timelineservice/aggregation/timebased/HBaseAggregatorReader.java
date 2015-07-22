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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HBaseAggregatorReader implements Closeable {
  private Configuration hbaseConf;
  private Connection conn;
  private EntityTable appLevelAggregationTable;

  private static final Log LOG = LogFactory.getLog(HBaseAggregatorReader.class);

  public void connect(Configuration conf) throws Exception{
    hbaseConf = HBaseConfiguration.create(conf);
    conn = ConnectionFactory.createConnection(hbaseConf);
    // TODO: read data from a different table than entity table.
    appLevelAggregationTable = new EntityTable();

  }

  @Override
  public void close() throws IOException {
    if (conn != null) {
      LOG.info("closing the hbase Connection");
      conn.close();
    }
  }

  public TimelineEntities getEntities(String clusterId, String userId,
      String flowName) throws IOException {

    byte[] startPrefix = getAppLevelAggregationRowKeyPrefix(clusterId,
        userId, flowName);
    Scan scan = new Scan();
    scan.setRowPrefixFilter(startPrefix);
    ResultScanner scanner = appLevelAggregationTable.getResultScanner(
        hbaseConf, conn, scan);
    TimelineEntities entities = new TimelineEntities();
    for (Result r : scanner) {
      TimelineEntity entity = getEntity(r);
      entities.addEntity(entity);
    }
    return entities;
  }

  private static byte[] getAppLevelAggregationRowKeyPrefix(
      String clusterId, String userId, String flowId) {
    return
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(userId, clusterId,
            flowId));
  }

  private static TimelineEntity getEntity(Result r) throws IOException {
    TimelineEntity entity = new TimelineEntity();
    entity.setType(EntityColumn.TYPE.readResult(r).toString());
    entity.setId(EntityColumn.ID.readResult(r).toString());
    entity.setCreatedTime(
        ((Number) EntityColumn.CREATED_TIME.readResult(r)).longValue());
    entity.setModifiedTime(
        ((Number) EntityColumn.MODIFIED_TIME.readResult(r)).longValue());

    // TODO: either branch this or directly call something similar in HBase reader
    NavigableMap<String, NavigableMap<Long, Number>> metricsResult =
        EntityColumnPrefix.METRIC.readTimeseriesResults(r);
    for (Map.Entry<String, NavigableMap<Long, Number>> metricResult:
        metricsResult.entrySet()) {
      TimelineMetric metric = new TimelineMetric();
      Collection<String> tokens =
          Separator.VALUES.splitEncoded(metricResult.getKey());
      if (tokens.size() != 2) {
        throw new IOException(
            "Invalid metric column name: " + metricResult.getKey());
      }
      Iterator<String> idItr = tokens.iterator();
      String id = idItr.next();
      String toAggregateStr = idItr.next();
      boolean toAggregate = toAggregateStr.equals("1") ? true : false;
      metric.setId(id);
      metric.setToAggregate(toAggregate);
      // Simply assume that if the value set contains more than 1 elements, the
      // metric is a TIME_SERIES metric, otherwise, it's a SINGLE_VALUE metric
      metric.setType(metricResult.getValue().size() > 1 ?
          TimelineMetric.Type.TIME_SERIES : TimelineMetric.Type.SINGLE_VALUE);
      metric.addValues(metricResult.getValue());
      entity.addMetric(metric);
    }
    return entity;
  }
}
