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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineWriterImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHBaseAggregatorReader {
  private static HBaseTestingUtility util;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util = new HBaseTestingUtility();
    util.startMiniCluster();
    AggregationTestUtil.createHBaseSchema(util);

    TimelineEntity entity =
        AggregationTestUtil.getTestAggregationTimelineEntity();
    TimelineEntities te = new TimelineEntities();
    te.addEntity(entity);
    HBaseTimelineWriterImpl hbi = null;
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      TimelineCollectorContext sampleContext = AggregationTestUtil.sampleContext1;
      hbi.write(sampleContext.getClusterId(), sampleContext.getUserId(),
          sampleContext.getFlowName(), sampleContext.getFlowVersion(),
          sampleContext.getFlowRunId(), sampleContext.getAppId(), false, te, null);
      hbi.stop();
    } finally {
      hbi.stop();
      hbi.close();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testHBaseAggregatorReader() throws Exception {
    TimelineCollectorContext sampleContext = AggregationTestUtil.sampleContext1;
    try (HBaseAggregatorReader reader = new HBaseAggregatorReader()) {
      Configuration c1 = util.getConfiguration();
      reader.connect(c1);

      TimelineEntities entities = reader.getEntities(sampleContext.getClusterId(),
          sampleContext.getUserId(), sampleContext.getFlowName());
      Assert.assertTrue(entities.getEntities().size() == 1);

      TimelineEntity entity = entities.getEntities().iterator().next();
      TimelineEntity sample =
        AggregationTestUtil.getTestAggregationTimelineEntity();
      AggregationTestUtil.verifyEntity(entity, sample);
    }
  }

}
