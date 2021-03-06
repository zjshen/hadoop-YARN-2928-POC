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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.server.timelineservice.storage.app2flow.App2FlowColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.app2flow.App2FlowRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.app2flow.App2FlowTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TypedBufferedMutator;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * This implements a hbase based backend for storing application timeline entity
 * information.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HBaseTimelineWriterImpl extends AbstractService implements
    TimelineWriter {

  private Connection conn;
  private TypedBufferedMutator<EntityTable> entityTable;
  private TypedBufferedMutator<App2FlowTable> app2FlowTable;

  private static final Log LOG = LogFactory
      .getLog(HBaseTimelineWriterImpl.class);

  public HBaseTimelineWriterImpl() {
    super(HBaseTimelineWriterImpl.class.getName());
  }

  public HBaseTimelineWriterImpl(Configuration conf) throws IOException {
    super(conf.get("yarn.application.id",
        HBaseTimelineWriterImpl.class.getName()));
  }

  /**
   * initializes the hbase connection to write to the entity table
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    Configuration hbaseConf = HBaseConfiguration.create(conf);
    conn = ConnectionFactory.createConnection(hbaseConf);
    entityTable = new EntityTable().getTableMutator(hbaseConf, conn);
    app2FlowTable = new App2FlowTable().getTableMutator(hbaseConf, conn);
  }

  /**
   * Stores the entire information in TimelineEntities to the timeline store.
   */
  @Override
  public TimelineWriteResponse write(String clusterId, String userId,
      String flowName, String flowVersion, long flowRunId, String appId,
      boolean newApp,TimelineEntities data,
      Map<String, TimelineMetric> aggregatedMetrics) 
      throws IOException {

    TimelineWriteResponse putStatus = new TimelineWriteResponse();
    byte[] rowKey;

    // data is possible to be null if writer only want to write aggregated 
    // metrics

    if (data != null) {
      for (TimelineEntity te : data.getEntities()) {

        // a set can have at most 1 null
        if (te == null) {
          continue;
        }

        rowKey =
            EntityRowKey.getRowKey(clusterId, userId, flowName, flowRunId, appId,
                te.getType(), te.getId());

        storeInfo(rowKey, te, flowVersion);
        storeEvents(rowKey, te.getEvents());
        storeConfig(rowKey, te.getConfigs());
        storeMetrics(rowKey, te.getMetrics());
        storeRelations(rowKey, te.getIsRelatedToEntities(),
            EntityColumnPrefix.IS_RELATED_TO);
        storeRelations(rowKey, te.getRelatesToEntities(),
            EntityColumnPrefix.RELATES_TO);

        LOG.info(te.getIdentifier() + " is written");
      }
      if (data.getEntities().size() > 0) {
        entityTable.flush();
      }
    }

    if (newApp) {
      rowKey = App2FlowRowKey.getRowKey(clusterId, appId);
      App2FlowColumn.FLOW_ID.store(rowKey, app2FlowTable, null, flowName);
      App2FlowColumn.FLOW_RUN_ID.store(rowKey, app2FlowTable, null, flowRunId);
      app2FlowTable.flush();
      LOG.info("clusterId=" + clusterId + ", appId=" + appId + ", flowId=" +
          flowName + ", flowRunId=" + flowRunId +" is written");

    }
    
    if (aggregatedMetrics != null) {
      // generate aggregation row
      TimelineEntity aggEntity = new TimelineEntity();
      aggEntity.setType(
          TimelineEntityType.YARN_APPLICATION_AGGREGATION.toString());
      aggEntity.setId(appId);
      rowKey = EntityRowKey.getRowKey(clusterId, userId, flowName, flowRunId,
          appId, aggEntity.getType(), aggEntity.getId());
      storeInfo(rowKey, aggEntity, flowVersion);
      for (TimelineMetric metric : aggregatedMetrics.values()) {
        LOG.info("Metrics is written:");
        LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(metric, true));
        storeMetrics(rowKey, Collections.singleton(metric));
      }
     /* for (TimelineMetric metric : aggregatedMetrics.values()) {
        storeAggregatedMetrics(rowKey, metric);
      }*/
    }
    
    return putStatus;
  }

  /**
   * Stores the Relations from the {@linkplain TimelineEntity} object
   */
  private void storeRelations(byte[] rowKey,
      Map<String, Set<String>> connectedEntities,
      EntityColumnPrefix entityColumnPrefix) throws IOException {
    for (Map.Entry<String, Set<String>> connectedEntity : connectedEntities
        .entrySet()) {
      // id3?id4?id5
      String compoundValue =
          Separator.VALUES.joinEncoded(connectedEntity.getValue());

      entityColumnPrefix.store(rowKey, entityTable, connectedEntity.getKey(),
          null, compoundValue);
    }
  }

  /**
   * Stores information from the {@linkplain TimelineEntity} object
   */
  private void storeInfo(byte[] rowKey, TimelineEntity te, String flowVersion)
      throws IOException {

    EntityColumn.ID.store(rowKey, entityTable, null, te.getId());
    EntityColumn.TYPE.store(rowKey, entityTable, null, te.getType());
    EntityColumn.CREATED_TIME.store(rowKey, entityTable, null,
        te.getCreatedTime());
    EntityColumn.MODIFIED_TIME.store(rowKey, entityTable, null,
        te.getModifiedTime());
    EntityColumn.FLOW_VERSION.store(rowKey, entityTable, null, flowVersion);
  }

  /**
   * stores the config information from {@linkplain TimelineEntity}
   */
  private void storeConfig(byte[] rowKey, Map<String, String> config)
      throws IOException {
    if (config == null) {
      return;
    }
    for (Map.Entry<String, String> entry : config.entrySet()) {
      EntityColumnPrefix.CONFIG.store(rowKey, entityTable, entry.getKey(),
          null, entry.getValue());
    }
  }
  
  
  /**
   * stores the aggregated metrics information
   * TODO move it out to a separated application table
   * TODO no get used in PoC.
   */
  private void storeAggregatedMetrics(byte[] rowKey, TimelineMetric metric)
      throws IOException {
    if (metric == null) {
      return;
    }
    for (Long timestamp : metric.getValues().keySet()) {
      EntityColumnPrefix.AGGREGATED_METRICS.store(rowKey, entityTable,
          metric.getId(), timestamp, metric.getValues().get(timestamp));
    }
  }

  /**
   * stores the {@linkplain TimelineMetric} information from the
   * {@linkplain TimelineEvent} object
   */
  private void storeMetrics(byte[] rowKey, Set<TimelineMetric> metrics)
      throws IOException {
    if (metrics != null) {
      for (TimelineMetric metric : metrics) {
        String metricColumnQualifier = metric.getId();
        String toAggregate = metric.getToAggregate() ? "1" : "0";
        
        Map<Long, Number> timeseries = metric.getValues();
        for (Map.Entry<Long, Number> timeseriesEntry : timeseries.entrySet()) {
          Long timestamp = timeseriesEntry.getKey();
          EntityColumnPrefix.METRIC.store(rowKey, entityTable,
              metricColumnQualifier, toAggregate, timestamp,
              timeseriesEntry.getValue());
        }
      }
    }
  }

  /**
   * Stores the events from the {@linkplain TimelineEvent} object
   */
  private void storeEvents(byte[] rowKey, Set<TimelineEvent> events)
      throws IOException {
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event != null) {
          String eventId = event.getId();
          if (eventId != null) {
            Map<String, Object> eventInfo = event.getInfo();
            if (eventInfo != null) {
              for (Map.Entry<String, Object> info : eventInfo.entrySet()) {
                // eventId?infoKey
                byte[] columnQualifierFirst =
                    Bytes.toBytes(Separator.VALUES.encode(eventId));
                byte[] compoundColumnQualifierBytes =
                    Separator.VALUES.join(columnQualifierFirst,
                        Bytes.toBytes(info.getKey()));
                // convert back to string to avoid additional API on store.
                String compoundColumnQualifier =
                    Bytes.toString(compoundColumnQualifierBytes);
                EntityColumnPrefix.EVENT.store(rowKey, entityTable,
                    compoundColumnQualifier, null, info.getValue());
              } // for info: eventInfo
            }
          }
        }
      } // event : events
    }
  }

  @Override
  public TimelineWriteResponse aggregate(TimelineCollectorContext context,
      TimelineEntity data, TimelineAggregationTrack track) throws IOException {
    return null;
  }

  /**
   * close the hbase connections The close APIs perform flushing and release any
   * resources held
   */
  @Override
  protected void serviceStop() throws Exception {
    if (entityTable != null) {
      LOG.info("closing entity table");
      // The close API performs flushing and releases any resources held
      entityTable.close();
    }
    if (app2FlowTable != null) {
      LOG.info("closing app_flow table");
      // The close API performs flushing and releases any resources held
      app2FlowTable.close();
    }
    if (conn != null) {
      LOG.info("closing the hbase Connection");
      conn.close();
    }
    super.serviceStop();
  }

}