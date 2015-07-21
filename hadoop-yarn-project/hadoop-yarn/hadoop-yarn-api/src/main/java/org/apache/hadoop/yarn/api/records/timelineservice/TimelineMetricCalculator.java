/*
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
package org.apache.hadoop.yarn.api.records.timelineservice;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

public class TimelineMetricCalculator {

  public static TimelineMetric substract(TimelineMetric metric1,
      TimelineMetric metric2, long timestamp) {
    if (metric1.getType().equals(TimelineMetric.Type.SINGLE_VALUE) &&
        metric2.getType().equals(TimelineMetric.Type.SINGLE_VALUE)) {
      Number n1 = metric1.retrieveSingleDataValue();
      Number n2 = metric2.retrieveSingleDataValue();
      TimelineMetric metric = new TimelineMetric();
      metric.setId(metric1.getId());
      metric.addValue(timestamp, sub(n1, n2));
      metric.setToAggregate(true);
      return metric;
    } else {
      throw new YarnRuntimeException("Substract operation on TimelineMetrics " +
          "only support single value type.");
    }
  }

  /**
   * 
   * @param n1
   * @param n2
   * @return
   */
  public static Number sub(Number n1, Number n2) {
    if (n1 == null) {
      throw new YarnRuntimeException("Number be substracted shouldn't be null.");
    } else if (n2 == null) {
      return n1;
    }

    if (n1 instanceof Integer){
      return new Integer(n1.intValue() - n2.intValue());
    }

    if (n1 instanceof Long) {
      return new Long(n1.longValue() - n2.longValue());
    }

    if (n1 instanceof Float) {
      return new Float(n1.floatValue() - n2.floatValue());
    }

    if (n1 instanceof Double) {
      return new Double(n1.doubleValue() - n2.doubleValue());
    }

    if (n1 instanceof Long) {
      return new Long(n1.longValue() - n2.longValue());
    }
    
    // TODO throw warnings/exceptions for other types of number.
    return null;
  }

  /**
   * 
   * @param n1
   * @param n2
   * @return
   */
  public static Number sum(Number n1, Number n2) {
    if (n1 == null) {
      return n2;
    } else if (n2 == null) {
      return n1;
    }

    if (n1 instanceof Integer){
      return new Integer(n1.intValue() + n2.intValue());
    }

    if (n1 instanceof Long) {
      return new Long(n1.longValue() + n2.longValue());
    }

    if (n1 instanceof Float) {
      return new Float(n1.floatValue() + n2.floatValue());
    }

    if (n1 instanceof Double) {
      return new Double(n1.doubleValue() + n2.doubleValue());
    }

    if (n1 instanceof Long) {
      return new Long(n1.longValue() + n2.longValue());
    }
    
    // TODO throw warnings/exceptions for other types of number.
    return null;
  }

}
