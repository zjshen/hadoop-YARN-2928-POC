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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollector;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Perform flow level time-based aggregation
 */
public class FlowAggregator {

  public static final String FLOW_AGGREGATOR_JOB_NAME
      = "timeline_v2_flow_aggregation";
  // TODO: replace this with default aggregation entity type
  @VisibleForTesting
  static final String FLOW_AGGREGATION_TYPE = "time_based_flow_aggregation";
  @VisibleForTesting
  static final String KEY_SEPARATOR = ";";
  private static final int KEY_IDX_CLUSTER_ID = 0;
  private static final int KEY_IDX_USER = 1;
  private static final int KEY_IDX_FLOW_NAME = 2;

  public static class FlowAggregatorMapper
      extends Mapper<LongWritable, Text, Text, TimelineEntityWritable>{

    @Override
    public void map(LongWritable key, Text flowInfo, Context context)
        throws IOException, InterruptedException {
      String[] rawContext = flowInfo.toString().split(KEY_SEPARATOR);
      String clusterId = rawContext[KEY_IDX_CLUSTER_ID];
      String user = rawContext[KEY_IDX_USER];
      String flowName = rawContext[KEY_IDX_FLOW_NAME];

      try {
        HBaseAggregatorReader reader = new HBaseAggregatorReader();
        reader.connect(context.getConfiguration());

        TimelineEntities entities
            = reader.getEntities(clusterId, user, flowName);
        TimelineEntity resultEntity = aggregateEntities(entities,
            context.getJobName(), FLOW_AGGREGATION_TYPE);

        context.write(flowInfo, new TimelineEntityWritable(resultEntity));
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public static class FlowAggregatorReducer
      extends Reducer<Text, TimelineEntityWritable, Text,
      TimelineEntityWritable> {

    @Override
    public void reduce(Text key, Iterable<TimelineEntityWritable> values,
        Context context)
        throws IOException, InterruptedException {
      PhoenixAggregatorStorage aggregatorStorage
          = new PhoenixAggregatorStorage();
      try {
        String[] rawContext = key.toString().split(KEY_SEPARATOR);
        String clusterId = rawContext[KEY_IDX_CLUSTER_ID];
        String user = rawContext[KEY_IDX_USER];
        String flowName = rawContext[KEY_IDX_FLOW_NAME];

        TimelineEntities entities = new TimelineEntities();
        Iterator valuesIterator = values.iterator();
        while (valuesIterator.hasNext()) {
          TimelineEntityWritable entityWritable
              = (TimelineEntityWritable) valuesIterator.next();
          TimelineEntity entity = entityWritable.get();
          entities.addEntity(entity);
        }

        TimelineEntity resultEntity = aggregateEntities(entities,
            context.getJobName(), FLOW_AGGREGATION_TYPE);
        TimelineEntities resultEntities = new TimelineEntities();
        resultEntities.addEntity(resultEntity);

        aggregatorStorage.serviceInit(context.getConfiguration());
        aggregatorStorage.writeFlowAggregation(clusterId, user, flowName,
            resultEntities);
        aggregatorStorage.serviceStop();
        context.write(key, new TimelineEntityWritable(resultEntity));
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  static int runAggregations(String[] args) throws Exception {
    Configuration conf = new Configuration();
    long timestamp = System.currentTimeMillis();
    Job job = Job.getInstance(conf, FLOW_AGGREGATOR_JOB_NAME + "_" + timestamp);
    job.setJarByClass(FlowAggregator.class);
    job.setMapperClass(FlowAggregatorMapper.class);
    job.setReducerClass(FlowAggregatorReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TimelineEntityWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    int returnCode = job.waitForCompletion(true) ? 0 : 1;

    return returnCode;
  }

  private static TimelineEntity aggregateEntities(TimelineEntities entities,
      String jobName, String aggregationType) {
    TimelineEntity resultEntity = new TimelineEntity();
    resultEntity.setId(jobName);
    resultEntity.setType(aggregationType);

    Map<String, TimelineMetric> aggregateMap = new HashMap<>();

    aggregateMap = TimelineCollector.aggregateMetrics(entities, aggregateMap,
        new HashMap<String, Number>(),
        new HashMap<String, Map<String, TimelineMetric>>());
    resultEntity.setMetrics(new HashSet<>(aggregateMap.values()));
    return resultEntity;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("FlowAggregator <input_path> <output_path>");
      return;
    }
    System.exit(runAggregations(args));
  }
}
