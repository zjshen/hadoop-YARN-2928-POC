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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

public class TestPhoenixAggregatorStorage extends BaseTest {
  private static PhoenixAggregatorStorage aggregatorStorage;
  private static final int BATCH_SIZE = 3;

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

  private static PhoenixAggregatorStorage setupPhoenixClusterAndWriterForTest(
      YarnConfiguration conf) throws Exception{
    Map<String, String> props = new HashMap<>();
    // Must update config before starting server
    props.put(QueryServices.STATS_USE_CURRENT_TIME_ATTRIB,
        Boolean.FALSE.toString());
    props.put("java.security.krb5.realm", "");
    props.put("java.security.krb5.kdc", "");
    props.put(IntegrationTestingUtility.IS_DISTRIBUTED_CLUSTER,
        Boolean.FALSE.toString());
    props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(5000));
    props.put(IndexWriterUtils.HTABLE_THREAD_KEY, Integer.toString(100));
    // Make a small batch size to test multiple calls to reserve sequences
    props.put(QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB,
        Long.toString(BATCH_SIZE));
    // Must update config before starting server
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));

    PhoenixAggregatorStorage myWriter = new PhoenixAggregatorStorage();
    // Change connection settings for test
    conf.set(
        PhoenixAggregatorStorage.TIMELINE_SERVICE_PHOENIX_STORAGE_CONN_STR,
        getUrl());
    myWriter.connProperties = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    myWriter.serviceInit(conf);
    return myWriter;
  }

  private static TimelineEntity getTestAggregationTimelineEntity() {
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

  private void testAggregator(AggregationStorageInfo aggregationInfo)
      throws Exception {
    // Set up a list of timeline entities and write them back to Phoenix
    int numEntity = 1;
    TimelineEntities te = new TimelineEntities();
    te.addEntity(getTestAggregationTimelineEntity());
    aggregatorStorage.writeAggregatedEntity("cluster_1", "user1", "testFlow", te,
        aggregationInfo);

    // TODO: read base on aggregation type
    TimelineEntity readEntity = aggregatorStorage.readFlowAggregatedEntity(
        "cluster_1", "user1", "testFlow");

    assertNotNull(readEntity);
    TimelineEntity sample = getTestAggregationTimelineEntity();

    assertEquals(readEntity.getCreatedTime(), sample.getCreatedTime());
    assertEquals(readEntity.getModifiedTime(), sample.getModifiedTime());

    TimelineMetric metric = readEntity.getMetrics().iterator().next();
    TimelineMetric sampleMetric = sample.getMetrics().iterator().next();
    assertEquals(metric.getId(), sampleMetric.getId());
    assertEquals(metric.getType(), sampleMetric.getType());

    Long metricKey = metric.getValues().keySet().iterator().next();
    Long sampleMetricKey =
        sampleMetric.getValues().keySet().iterator().next();
    assertEquals(metricKey, sampleMetricKey);
    assertEquals(metric.getValues().get(metricKey),
        sampleMetric.getValues().get(sampleMetricKey));

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


  private void verifySQLWithCount(String sql, int targetCount, String message)
      throws Exception {
    try (
        Statement stmt = aggregatorStorage.getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      assertTrue("Result set empty on statement " + sql, rs.next());
      assertNotNull("Fail to execute query " + sql, rs);
      assertEquals(message + " " + targetCount, targetCount, rs.getInt(1));
    } catch (SQLException se) {
      fail("SQL exception on query: " + sql
          + " With exception message: " + se.getLocalizedMessage());
    }
  }
}
