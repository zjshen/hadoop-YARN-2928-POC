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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.junit.Assert;
import org.junit.Test;

public class TestTimelineEntityWritable {
  @Test
  public void testTimelineEntityWritable() throws Exception {
    int testListSize = 3;
    TimelineEntities entityList =
        AggregationTestUtil.getStandardTestTimelineEntities(testListSize);
    for (TimelineEntity entity : entityList.getEntities()) {
      checkTimelineEntityWritable(new TimelineEntityWritable(entity));
    }
  }

  private void checkTimelineEntityWritable(TimelineEntityWritable before)
      throws Exception {
    DataOutputBuffer dob = new DataOutputBuffer();
    before.write(dob);

    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), dob.getLength());

    TimelineEntityWritable after = new TimelineEntityWritable();
    after.readFields(dib);

    verifyEntityEquivalance(before.get(), after.get());
  }

  private void verifyEntityEquivalance(TimelineEntity e1, TimelineEntity e2) {
    if (e1 == e2) {
      return;
    }
    if (e1 == null) {
      Assert.fail("Incoming entity 1 is null! ");
    }
    if (!e1.getId().equals(e2.getId())) {
      Assert.fail("id mismatch! ");
    }
    if (!e1.getType().equals(e2.getType())) {
      Assert.fail("type mismatch!");
    }
/*    if (!e1.getEvents().equals(e2.getEvents())) {
      Assert.fail("events mismatch! ");
    }
    if (!e1.getInfo().equals(e2.getInfo())) {
      Assert.fail("info mismatch! ");
    }
    if (!e1.getConfigs().equals(e2.getConfigs())) {
      Assert.fail("config mismatch! ");
    }*/
    if (!e1.getMetrics().equals(e2.getMetrics())) {
      Assert.fail("metrics mismatch! ");
    }
    /*if (!e1.getIsRelatedToEntities().equals(e2.getIsRelatedToEntities())) {
      Assert.fail("isRelatedTo mismatch! ");
    }
    if (!e1.getRelatesToEntities().equals(e2.getRelatesToEntities())) {
      Assert.fail("relatesTo mismatch! ");
    }*/
    if (e1.getCreatedTime() != e2.getCreatedTime()) {
      Assert.fail("createdTime mismatch! ");
    }
    if (e1.getModifiedTime() != e2.getModifiedTime()) {
      Assert.fail("modifiedTime mismatch! ");
    }
  }

}
