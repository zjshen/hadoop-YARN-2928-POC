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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineWriterImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestFlowAggregator extends PhoenixRelatedTest {

  HBaseTestingUtility util;
  YarnConfiguration confForPhoenix;

  @Before
  public void setup() throws Exception {
    // setup Phoenix
    confForPhoenix = new YarnConfiguration();
    aggregatorStorage = setupPhoenixClusterAndWriterForTest(confForPhoenix);
    // setup hbase
    util = getUtility();
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

  @Test(timeout = 90000)
  public void testFlowLevelAggregation() throws Exception {
    TimelineCollectorContext sampleContext = AggregationTestUtil.sampleContext1;

    // test mapper
    Mapper.Context mapperContext = mock(Mapper.Context.class);
    // return conf for HBase here
    when(mapperContext.getConfiguration()).thenReturn(util.getConfiguration());
    when(mapperContext.getJobName()).thenReturn("test_job_name");

    FlowAggregator.FlowAggregatorMapper testMapper =
        new FlowAggregator.FlowAggregatorMapper();
    String sampleInput
        = sampleContext.getClusterId() + FlowAggregator.KEY_SEPARATOR
        + sampleContext.getUserId() + FlowAggregator.KEY_SEPARATOR
        + sampleContext.getFlowName();
    testMapper.map(new LongWritable(1), new Text(sampleInput), mapperContext);

    ArgumentCaptor<Text> outputFlowInfo = ArgumentCaptor.forClass(Text.class);
    ArgumentCaptor<TimelineEntityWritable> outputWritable
        = ArgumentCaptor.forClass(TimelineEntityWritable.class);
    verify(mapperContext).write(outputFlowInfo.capture(),
        outputWritable.capture());

    TimelineEntityWritable mapperOutput = outputWritable.getValue();
    assertEquals(FlowAggregator.FLOW_AGGREGATION_TYPE,
        mapperOutput.get().getType());
    assertEquals(AggregationTestUtil.getTestAggregationTimelineEntity()
        .getMetrics().size() * 2, mapperOutput.get().getMetrics().size());


    // test reducer
    Reducer.Context reducerContext = mock(Reducer.Context.class);
    // return conf for Phoenix here
    when(reducerContext.getConfiguration()).thenReturn(confForPhoenix);
    when(reducerContext.getJobName()).thenReturn("test_job_name");

    final List entityWritableList = new ArrayList();
    entityWritableList.add(mapperOutput);
    FlowAggregator.FlowAggregatorReducer testReducer
        = new FlowAggregator.FlowAggregatorReducer();
    testReducer.reduce(new Text(sampleInput),
        new Iterable<TimelineEntityWritable>() {
          @Override public Iterator<TimelineEntityWritable> iterator() {
            return entityWritableList.iterator();
          }
        },
        reducerContext);

    // Verify if we're storing all entities
    TimelineEntity readEntity = aggregatorStorage.readFlowAggregatedEntity(
        sampleContext.getClusterId(), sampleContext.getUserId(),
        sampleContext.getFlowName());
    assertNotNull(readEntity);

    AggregationStorageInfo aggregationInfo
        = AggregationStorageInfo.FLOW_AGGREGATION;
    String[] primaryKeyList = aggregationInfo.getPrimaryKeyList();
    String sql = "SELECT COUNT(" + primaryKeyList[primaryKeyList.length - 1]
        +") FROM " + aggregationInfo.getTableName();
    verifySQLWithCount(sql, 1, "Number of entities should be ");
    sql = "SELECT COUNT(m.HDFS_BYTES_READ) FROM "
        + aggregationInfo.getTableName() + "(m.HDFS_BYTES_READ VARBINARY) ";
    verifySQLWithCount(sql, 1,
        "Number of entities with info should be ");
  }

  @After
  public void cleanup() throws Exception {
    aggregatorStorage.dropTable(
        AggregationStorageInfo.FLOW_AGGREGATION_TABLE_NAME);
    aggregatorStorage.dropTable(
        AggregationStorageInfo.USER_AGGREGATION_TABLE_NAME);
    aggregatorStorage.serviceStop();
    tearDownMiniCluster();
  }
}
