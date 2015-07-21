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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricCalculator;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * Service that handles writes to the timeline service and writes them to the
 * backing storage.
 *
 * Classes that extend this can add their own lifecycle management or
 * customization of request handling.
 */
@Private
@Unstable
public abstract class TimelineCollector extends CompositeService {
  private static final Log LOG = LogFactory.getLog(TimelineCollector.class);

  private TimelineWriter writer;

  // <metric_id, <entity_id, metric>>
  private Map<String, Map<String, TimelineMetric>> cachedLatestMetrics =
      new HashMap<String, Map<String, TimelineMetric>>();

  // <metric_id, aggregated_metric>
  private Map<String, TimelineMetric> perIdAggregatedMetricsArea =
      new HashMap<String, TimelineMetric>();

  // <metric_id, aggregated_metric_num>
  private Map<String, Number> perIdAggregatedNum =
      new HashMap<String, Number>();

  public TimelineCollector(String name) {
    super(name);
  }

  public static String AREA_POSTFIX = "_AREA";

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  protected void setWriter(TimelineWriter w) {
    this.writer = w;
  }

  public TimelineWriteResponse putEntities(TimelineEntities entities,
      UserGroupInformation callerUgi) throws IOException {
    return putEntities(entities, false, callerUgi);
  }

  /**
   * Handles entity writes. These writes are synchronous and are written to the
   * backing storage without buffering/batching. If any entity already exists,
   * it results in an update of the entity.
   *
   * This method should be reserved for selected critical entities and events.
   * For normal voluminous writes one should use the async method
   * {@link #putEntitiesAsync(TimelineEntities, boolean, UserGroupInformation)}.
   *
   * @param entities entities to post
   * @param newApp the flag indicate the start of a new app
   * @param callerUgi the caller UGI
   * @return the response that contains the result of the post.
   */
  public TimelineWriteResponse putEntities(TimelineEntities entities,
      boolean newApp, UserGroupInformation callerUgi) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("SUCCESS - TIMELINE V2 PROTOTYPE");
      LOG.debug("putEntities(entities=" + entities + ", newApp =" + newApp +
          ", callerUgi="  + callerUgi + ")");
    }

    TimelineCollectorContext context = getTimelineEntityContext();
    Map<String, TimelineMetric> aggregatedMetrics =
        aggregateMetrics(entities);
    LOG.info("Metrics to aggregate:");
    for (Map.Entry<String, TimelineMetric> entry :
        aggregatedMetrics.entrySet()) {
      LOG.info(entry.getKey() + " : " +
          TimelineUtils.dumpTimelineRecordtoJSON(entry.getValue()));
    }
    return writer.write(context.getClusterId(), context.getUserId(),
        context.getFlowName(), context.getFlowVersion(), context.getFlowRunId(),
        context.getAppId(), newApp, entities, aggregatedMetrics);
  }
  
  /**
   * Aggregate Metrics with entities.
   * @param entities
   * @return
   */
  private Map<String, TimelineMetric> aggregateMetrics(
      TimelineEntities entities) {
    return TimelineCollector.aggregateMetrics(entities,
        this.perIdAggregatedMetricsArea, this.perIdAggregatedNum,
        cachedLatestMetrics);
  }

  /**
   * Aggregate Metrics with entities against base aggregation metrics and 
   * cached latest metrics.
   * @param entities
   * @param baseAggregatedArea
   * @param cachedLatestMetrics cached latest metrics mapping from 
   * metric_id to <entity_id, metric>
   * @return
   */
  public static Map<String, TimelineMetric> aggregateMetrics(
      TimelineEntities entities,
      Map<String, TimelineMetric> baseAggregatedArea,
      Map<String, Number> perIdAggregatedNum,
      Map<String, Map<String, TimelineMetric>> cachedLatestMetrics) {

    Map<String, TimelineMetric> modifiedMetricMap =
        new HashMap<String, TimelineMetric>();

    Set<TimelineEntity> entitySet = entities.getEntities();
    for (TimelineEntity entity : entitySet) {
      String entityId = entity.getId();
      Set<TimelineMetric> timelineMetricSet = entity.getMetrics();
      if (timelineMetricSet != null && !timelineMetricSet.isEmpty()) {
        for (TimelineMetric metric : timelineMetricSet) {
          // do nothing for null metric
          if (metric != null) {
            String metricId = metric.getId();
            Map<String, TimelineMetric> entityIdMap = 
                cachedLatestMetrics.get(metricId);

            // first metrics aggregated on specific metric_id
            if (entityIdMap == null) {
              entityIdMap = new HashMap<String, TimelineMetric>();
              cachedLatestMetrics.put(metricId, entityIdMap);
              perIdAggregatedNum.put(metricId, null);
            }
            TimelineMetric latestTimelineMetrics = entityIdMap.get(entityId);

            Number delta = null;
            // new added metric for specific entityId
            if (latestTimelineMetrics == null) {
              delta = metric.retrieveSingleDataValue();
            } else {
              delta = TimelineMetricCalculator.sub(
                  metric.retrieveSingleDataValue(), 
                  latestTimelineMetrics.retrieveSingleDataValue());
            }

            Number aggregatedNum = perIdAggregatedNum.get(metricId);
            if (aggregatedNum == null) {
              aggregatedNum = delta;
            } else {
              aggregatedNum = TimelineMetricCalculator.sum(delta, 
                  aggregatedNum);
            }
            perIdAggregatedNum.put(metricId, aggregatedNum);

            TimelineMetric oldAggregatedArea =
                baseAggregatedArea.get(metricId);

            // Record aggregation time.
            long aggregatedTime = System.currentTimeMillis();
            TimelineMetric newAggregatedMetrics = new TimelineMetric();
            newAggregatedMetrics.setId(metricId);
            newAggregatedMetrics.setToAggregate(false);
            newAggregatedMetrics.addValue(aggregatedTime, aggregatedNum);

            TimelineMetric newAggregatedArea = metric.aggregateTo(
                oldAggregatedArea, latestTimelineMetrics, aggregatedTime,
                TimelineMetric.Operation.SUM);

            // cache aggregated metrics globally
            baseAggregatedArea.put(metricId, newAggregatedMetrics);

            // aggregated metrics updated on specific metricId
            modifiedMetricMap.put(metricId, newAggregatedMetrics);
            modifiedMetricMap.put(metricId + AREA_POSTFIX,
                newAggregatedArea);

            // update entityIdMap (cachedLatestMetrics) with latest metric on 
            // specific entityId
            entityIdMap.put(entityId, metric.retrieveLatestSingleValueMetric());
          }
        }
      }
    }
    return modifiedMetricMap;
  }

  public void putEntitiesAsync(TimelineEntities entities,
      UserGroupInformation callerUgi) {
    putEntitiesAsync(entities, false, callerUgi);
  }

  /**
   * Handles entity writes in an asynchronous manner. The method returns as soon
   * as validation is done. No promises are made on how quickly it will be
   * written to the backing storage or if it will always be written to the
   * backing storage. Multiple writes to the same entities may be batched and
   * appropriate values updated and result in fewer writes to the backing
   * storage.
   *
   * @param entities entities to post
   * @param newApp the flag indicate the start of a new app
   * @param callerUgi the caller UGI
   */
  public void putEntitiesAsync(TimelineEntities entities,
      boolean newApp, UserGroupInformation callerUgi) {
    // TODO implement
    if (LOG.isDebugEnabled()) {
      LOG.debug("putEntitiesAsync(entities=" + entities + ", newApp =" +
          newApp +", callerUgi=" + callerUgi + ")");
    }
  }

  public abstract TimelineCollectorContext getTimelineEntityContext();

}
