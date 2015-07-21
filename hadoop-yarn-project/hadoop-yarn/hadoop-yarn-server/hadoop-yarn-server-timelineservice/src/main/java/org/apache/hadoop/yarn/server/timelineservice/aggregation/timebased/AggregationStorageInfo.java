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
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;

import java.sql.PreparedStatement;
import java.sql.SQLException;

final class AggregationStorageInfo {
  /**
   * Default flow level aggregation table name
   */
  @VisibleForTesting
  static final String FLOW_AGGREGATION_TABLE_NAME
      = "yarn_timeline_flow_aggregation";
  /**
   * Default user level aggregation table name
   */
  @VisibleForTesting
  static final String USER_AGGREGATION_TABLE_NAME
      = "yarn_timeline_user_aggregation";

  // These lists are not taking effects in table creations.
  private static final String[] FLOW_AGGREGATION_PK_LIST =
      { "user", "cluster", "flow_name" };
  private static final String[] USER_AGGREGATION_PK_LIST = { "user" };

  private String tableName;
  private String[] primaryKeyList;
  private PrimaryKeyStringSetter primaryKeyStringSetter;

  private AggregationStorageInfo(String table, String[] pkList,
      PrimaryKeyStringSetter formatter) {
    tableName = table;
    primaryKeyList = pkList;
    primaryKeyStringSetter = formatter;
  }

  private interface PrimaryKeyStringSetter {
    int setValues(PreparedStatement ps, TimelineCollectorContext context,
        String[] extraInfo, int startPos) throws SQLException;
  }

  public String getTableName() {
    return tableName;
  }

  public String[] getPrimaryKeyList() {
    return primaryKeyList;
  }

  public int setStringsForPrimaryKey(PreparedStatement ps,
      TimelineCollectorContext context, String[] extraInfo, int startPos)
      throws SQLException {
    return primaryKeyStringSetter.setValues(ps, context, extraInfo, startPos);
  }

  public static final AggregationStorageInfo FLOW_AGGREGATION =
      new AggregationStorageInfo(FLOW_AGGREGATION_TABLE_NAME,
          FLOW_AGGREGATION_PK_LIST, new PrimaryKeyStringSetter() {
        @Override
        public int setValues(PreparedStatement ps,
            TimelineCollectorContext context, String[] extraInfo, int startPos)
            throws SQLException {
          int idx = startPos;
          ps.setString(idx++, context.getUserId());
          ps.setString(idx++, context.getClusterId());
          ps.setString(idx++, context.getFlowName());
          return idx;
        }
      });

  public static final AggregationStorageInfo USER_AGGREGATION =
      new AggregationStorageInfo(USER_AGGREGATION_TABLE_NAME,
          USER_AGGREGATION_PK_LIST, new PrimaryKeyStringSetter() {
        @Override
        public int setValues(PreparedStatement ps,
            TimelineCollectorContext context, String[] extraInfo, int startPos)
            throws SQLException {
          int idx = startPos;
          ps.setString(idx++, context.getUserId());
          return idx;
        }
      });
}
