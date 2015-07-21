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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Serialization wrapper for timeline entities
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class TimelineEntityWritable implements Writable {

  private TimelineEntity entityData;

  public TimelineEntityWritable() {}

  public TimelineEntityWritable(TimelineEntity entity) {
    set(entity);
  }

  public void set(TimelineEntity entity) {
    entityData = entity;
  }

  public TimelineEntity get() {
    return entityData;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(entityData.getId());
    out.writeUTF(entityData.getType());
    out.writeLong(entityData.getCreatedTime());
    out.writeLong(entityData.getModifiedTime());
    // For aggregations only. Only stores metric data
    out.writeInt(entityData.getMetrics().size());
    for (TimelineMetric m : entityData.getMetrics()) {
      WritableUtils.writeEnum(out, m.getType());
      out.writeUTF(m.getId());
      writeMetricValues(m, out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (entityData == null) {
      entityData = new TimelineEntity();
    }
    entityData.setId(in.readUTF());
    entityData.setType(in.readUTF());
    entityData.setCreatedTime(in.readLong());
    entityData.setModifiedTime(in.readLong());
    int numMetrics = in.readInt();
    for (int i = 0; i < numMetrics; i++) {
      TimelineMetric m = new TimelineMetric();
      m.setType(WritableUtils.readEnum(in, TimelineMetric.Type.class));
      m.setId(in.readUTF());
      readMetricValues(m, in);
    }
  }

  private static void writeMetricValues(TimelineMetric m, DataOutput out)
      throws IOException{
    MapWritable writableValues = new MapWritable();
    Map<Long, Number> values = m.getValues();
    if (values != null) {
      for (Map.Entry<Long, Number> entry : values.entrySet()) {
        writableValues.put(new LongWritable(entry.getKey()),
            new BytesWritable(GenericObjectMapper.write(entry.getValue())));
      }
    }
    writableValues.write(out);
  }

  private static void readMetricValues(TimelineMetric m, DataInput in)
      throws IOException {
    MapWritable resultWritable = new MapWritable();
    resultWritable.readFields(in);
    for (Map.Entry<Writable, Writable> entryWritable :
        resultWritable.entrySet()) {
      Long key = ((LongWritable) entryWritable.getKey()).get();
      Number value = (Number) GenericObjectMapper.read(
          ((BytesWritable) entryWritable.getValue()).getBytes());
      m.addValue(key, value);
    }
  }

}
