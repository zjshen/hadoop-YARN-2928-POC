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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@XmlRootElement(name = "metric")
@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TimelineMetric {

  public static enum Type {
    SINGLE_VALUE,
    TIME_SERIES
  }

  // List of operations that aggregation of metric should support.
  public static enum Operation {
    SUM, // SUM UP metric data
    AVG, // TODO not support yet
    REP // The new metric data replace the old one
  }

  private Type type;
  private String id;

  // the metrics need to be added on an existing summary
  // default to be false
  private boolean toAggregate = false;

  private Comparator<Long> reverseComparator = new Comparator<Long>() {
    @Override
    public int compare(Long l1, Long l2) {
      return l2.compareTo(l1);
    }
  };
  private TreeMap<Long, Number> values = new TreeMap<>(reverseComparator);

  public TimelineMetric() {
    this(Type.SINGLE_VALUE);
  }

  public TimelineMetric(Type type) {
    this.type = type;
  }


  @XmlElement(name = "type")
  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  @XmlElement(name = "id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
  
  
  @XmlElement(name = "aggregated")
  public boolean getToAggregate() {
    return toAggregate;
  }

  public void setToAggregate(boolean toAggregate) {
    this.toAggregate = toAggregate;
  }

  /**
   * Support SUM and REP now, but will be extended later.
   * For SUM, the aggregation will calculate delta from latestMetric (t1, n1) to
   * current metric (t2, n2) as (n2+n1)/2*(n2-n1) and apply it on previous
   * aggregated metric (baseAggregatedMetric).
   * The assumption here is baseAggregatedMetric and latestMetric should be 
   * single value data if not null.
   * @param baseAggregatedMetric
   * @param latestMetric
   * @param aggregateTime
   * @param op
   * @return
   */
  public TimelineMetric aggregateTo(TimelineMetric baseAggregatedMetric, 
      TimelineMetric latestMetric, long aggregateTime, Operation op) {

    TimelineMetric aggregatedMetric = new TimelineMetric();
    aggregatedMetric.setId(this.id);
    Number aggregatedValue = null;
    // 
    if (op.equals(Operation.SUM)) {
      Long t1 = null;
      Number n1 = null;

      // Get time and value for last update
      if (latestMetric != null) {
        if (!latestMetric.getType().equals(Type.SINGLE_VALUE)) {
          throw new RuntimeException("It is not a single value for latest " +
              "metrics: " + baseAggregatedMetric);
        }
        t1 = latestMetric.getValuesJAXB().firstKey();
        if (t1 != null) {
          n1 = latestMetric.getValues().get(t1);
        }
      }

      for (Map.Entry<Long, Number> timeSeriesData : 
          this.getValues().entrySet()) {
        Long t2 = timeSeriesData.getKey();
        Number n2 = timeSeriesData.getValue();
        Number delta = null;
        // t1 = null means this is the first value get aggregated, delta should 
        // be null.
        if (t1 != null) {
          delta = delta(n1, n2, (t2 - t1));
        }

        aggregatedValue = sum(aggregatedValue, delta);
        // rebase time and value
        t1 = t2;
        n1 = n2;
      }

      // apply new delta to previous aggregated data.
      if (baseAggregatedMetric != null) {
        if (!baseAggregatedMetric.getType().equals(Type.SINGLE_VALUE)) {
          throw new RuntimeException("It is not a single value for aggregated " +
              "metrics: " + baseAggregatedMetric);
        }

        Long baseTime = baseAggregatedMetric.getValuesJAXB().firstKey();
        if (baseTime != null) {
          Number baseNumber = baseAggregatedMetric.getValues().get(baseTime);
          aggregatedValue = sum(aggregatedValue, baseNumber);
        }
      }
    } else if (op.equals(Operation.SUM)) {
      List<Number> valueList = new ArrayList<Number>(this.values.values());
      // Just use latest value to replace previous aggregate value
      aggregatedValue = valueList.get(0);
    } else {
      throw new YarnRuntimeException("Operations other than SUM or REP are " +
          "not supported yet!");
    }

    aggregatedMetric.addValue(aggregateTime, aggregatedValue);
    return aggregatedMetric;
  }

  // get latest timeline metric as single value type
  public TimelineMetric retrieveLatestSingleValueMetric() {
    if (this.getType().equals(Type.SINGLE_VALUE)) {
      return this;
    } else {
      TimelineMetric singleValueMetric = new TimelineMetric(Type.SINGLE_VALUE);
      Long lastKey = this.values.lastKey();
      if (lastKey != null) {
        Number lastValue = this.values.get(lastKey);
        singleValueMetric.addValue(lastKey, lastValue);
      }
      return singleValueMetric;
    }
  }

  // sum of two Numbers
  // TODO make it a static method.
  private Number sum(Number n1, Number n2) {
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
  
  /**
   * Delta calculation: (n2 + n1)/2 * time
   * assumptions: n2 and time are not null
   * @param n1
   * @param n2
   * @param time
   * @return
   */
  // TODO make it a static method
  private Number delta(Number n1, Number n2, Long time) {

    if (n2 instanceof Integer) {
      return new Long(
          (n1 == null ? 0 : n1.intValue() + n2.intValue()) * time / 2);
    }

    if (n1 instanceof Long) {
      return new Long(
          (n1 == null ? 0 : n1.longValue() + n2.longValue()) * time / 2);
    }

    if (n1 instanceof Float) {
      return new Float(
          (n1 == null ? 0 : n1.floatValue() + n2.floatValue()) * time / 2);
    }

    if (n1 instanceof Double) {
      return new Double(
          (n1 == null ? 0 : n1.doubleValue() + n2.doubleValue()) * time / 2);
    }

    if (n1 instanceof Long) {
      return new Long(
          (n1 == null ? 0 : n1.longValue() + n2.longValue()) * time / 2);
    }

    // TODO throw warnings/exceptions for other types of number.
    return null;
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "values")
  public TreeMap<Long, Number> getValuesJAXB() {
    return values;
  }

  public Map<Long, Number> getValues() {
    return values;
  }

  public long retrieveSingleDataKey() {
    if (this.type.equals(Type.SINGLE_VALUE)) {
      if (values.size() == 0) {
          throw new YarnRuntimeException("Values for this timeline metric is " +
              "empty.");
      } else {
        return values.firstKey();
      }
    } else {
      throw new YarnRuntimeException("Type for this timeline metric is not " +
          "SINGLE_VALUE.");
    }
  }

  public Number retrieveSingleDataValue() {
    if (this.type.equals(Type.SINGLE_VALUE)) {
      if (values.size() == 0) {
          return null;
      } else {
        return values.get(values.firstKey());
      }
    } else {
      throw new YarnRuntimeException("Type for this timeline metric is not " +
          "SINGLE_VALUE.");
    }
  }

  public void setValues(Map<Long, Number> values) {
    if (type == Type.SINGLE_VALUE) {
      overwrite(values);
    } else {
      if (values != null) {
        this.values = new TreeMap<Long, Number>(reverseComparator);
        this.values.putAll(values);
      } else {
        this.values = null;
      }
    }
  }

  public void addValues(Map<Long, Number> values) {
    if (type == Type.SINGLE_VALUE) {
      overwrite(values);
    } else {
      this.values.putAll(values);
    }
  }

  public void addValue(long timestamp, Number value) {
    if (type == Type.SINGLE_VALUE) {
      values.clear();
    }
    values.put(timestamp, value);
  }

  private void overwrite(Map<Long, Number> values) {
    if (values.size() > 1) {
      throw new IllegalArgumentException(
          "Values cannot contain more than one point in " +
              Type.SINGLE_VALUE + " mode");
    }
    this.values.clear();
    this.values.putAll(values);
  }

  public boolean isValid() {
    return (id != null);
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + type.hashCode();
    return result;
  }

  // Only check if type and id are equal
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof TimelineMetric))
      return false;

    TimelineMetric m = (TimelineMetric) o;

    if (!id.equals(m.id)) {
      return false;
    }
    if (type != m.type) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(id).append( " : ").append(type).append(" : toAggregate? : ").
        append(toAggregate).append(" : ").append(values.toString());
    return sb.toString();
  }

}
