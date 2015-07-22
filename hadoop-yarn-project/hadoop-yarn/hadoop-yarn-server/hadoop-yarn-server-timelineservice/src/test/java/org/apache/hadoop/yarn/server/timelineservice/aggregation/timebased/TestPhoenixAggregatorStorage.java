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

import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPhoenixAggregatorStorage extends PhoenixRelatedTest {

  @BeforeClass
  public static void setup() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    aggregatorStorage = setupPhoenixClusterAndWriterForTest(conf);
  }

  @Test(timeout = 90000)
  public void testFlowLevelAggregationStorage() throws Exception {
    testAggregator(AggregationStorageInfo.FLOW_AGGREGATION);
  }

  @Test(timeout = 90000)
  public void testUserLevelAggregationStorage() throws Exception {
    testAggregator(AggregationStorageInfo.USER_AGGREGATION);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    aggregatorStorage.dropTable(AggregationStorageInfo.FLOW_AGGREGATION_TABLE_NAME);
    aggregatorStorage.dropTable(AggregationStorageInfo.USER_AGGREGATION_TABLE_NAME);
    aggregatorStorage.serviceStop();
    tearDownMiniCluster();
  }


  private void testAggregator(AggregationStorageInfo aggregationInfo)
      throws Exception {
    // Set up a list of timeline entities and write them back to Phoenix
    int numEntity = 1;
    TimelineEntities te = new TimelineEntities();
    te.addEntity(AggregationTestUtil.getTestAggregationTimelineEntity());
    aggregatorStorage.writeAggregatedEntity("cluster_1", "user1", "testFlow",
        te,
        aggregationInfo);

    // TODO: read base on aggregation type
    TimelineEntity readEntity = aggregatorStorage.readFlowAggregatedEntity(
        "cluster_1", "user1", "testFlow");

    assertNotNull(readEntity);
    TimelineEntity sample
        = AggregationTestUtil.getTestAggregationTimelineEntity();
    AggregationTestUtil.verifyEntity(readEntity, sample);

    // Verify if we're storing all entities
    String[] primaryKeyList = aggregationInfo.getPrimaryKeyList();
    String sql = "SELECT COUNT(" + primaryKeyList[primaryKeyList.length - 1]
        +") FROM " + aggregationInfo.getTableName();
    verifySQLWithCount(sql, numEntity, "Number of entities should be ");
    // Check info (half of all entities)
    sql = "SELECT COUNT(i.info1) FROM "
        + aggregationInfo.getTableName() + "(i.info1 VARBINARY) ";
    verifySQLWithCount(sql, numEntity,
        "Number of entities with info should be ");
    sql = "SELECT COUNT(m.HDFS_BYTES_READ) FROM "
        + aggregationInfo.getTableName() + "(m.HDFS_BYTES_READ VARBINARY) ";
    verifySQLWithCount(sql, numEntity,
        "Number of entities with info should be ");
  }
}
