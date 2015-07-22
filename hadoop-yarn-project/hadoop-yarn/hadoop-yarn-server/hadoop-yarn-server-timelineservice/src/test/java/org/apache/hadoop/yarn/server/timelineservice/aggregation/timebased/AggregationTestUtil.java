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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationTestUtil {

  public static final TimelineCollectorContext sampleContext1 =
      new TimelineCollectorContext("cluster1", "user1", "some_flow_name",
          "AB7822C10F1111", 1002345678919L, "some app name");

  public static TimelineEntities getStandardTestTimelineEntities(int size) {
    TimelineEntities entities = new TimelineEntities();
    for (int i = 0; i < size; i++) {
      TimelineEntity entity = new TimelineEntity();
      String id = "hello" + i;
      String type = "testAggregationType";
      entity.setId(id);
      entity.setType(type);
      entity.setCreatedTime(1425016501000L + i);
      entity.setModifiedTime(1425016502000L + i);

      entity.addInfo("info1", new Integer(i));
      entity.addInfo("info2", "helloworld" + i);

      TimelineMetric metric = new TimelineMetric();
      metric.setId("HDFS_BYTES_READ");
      metric.addValue(1425016501100L + i, 8000 + i);
      entity.addMetric(metric);
    }

    return entities;
  }

  public static TimelineEntity getTestAggregationTimelineEntity() {
    TimelineEntity entity = new TimelineEntity();
    String id = "hello1";
    String type = "testAggregationType";
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(1425016501000L);
    entity.setModifiedTime(1425016502000L);

    entity.addInfo("info1", new Integer(1));
    entity.addInfo("info2", "helloworld");

    TimelineMetric metric = new TimelineMetric();
    metric.setId("HDFS_BYTES_READ");
    metric.addValue(1425016501100L, 8000);
    entity.addMetric(metric);

    TimelineMetric metric1 = new TimelineMetric();
    metric1.setId("CPU");
    metric1.addValue(1425016501100L, 6);
    entity.addMetric(metric1);

    return entity;
  }

  public static void createHBaseSchema(HBaseTestingUtility util)
      throws IOException {
    new EntityTable()
        .createTable(util.getHBaseAdmin(), util.getConfiguration());
  }

  public static void verifyEntity(TimelineEntity entity, TimelineEntity sample) {
    assertEquals(entity.getCreatedTime(), sample.getCreatedTime());
    assertEquals(entity.getModifiedTime(), sample.getModifiedTime());

    assertEquals(entity.getMetrics().size(), sample.getMetrics().size());
    for (TimelineMetric m : entity.getMetrics()) {
      assertTrue(sample.getMetrics().contains(m));
    }
  }
}
