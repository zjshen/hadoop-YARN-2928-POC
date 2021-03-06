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
package org.apache.hadoop.yarn.server.timelineservice.storage.entity;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * Represents a rowkey for the entity table.
 */
public class EntityRowKey {
  // TODO: more methods are needed for this class.

  // TODO: API needs to be cleaned up.

  /**
   * Constructs a row key prefix for the entity table as follows:
   * {@code userName!clusterId!flowId!flowRunId!AppId}
   *
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @param appId
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKeyPrefix(String clusterId, String userId,
      String flowId, Long flowRunId, String appId) {
    byte[] first =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(userId, clusterId,
            flowId));
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(EntityRowKey.invert(flowRunId));
    byte[] third = Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(appId));
    return Separator.QUALIFIERS.join(first, second, third);
  }

  /**
   * Constructs a row key prefix for the entity table as follows:
   * {@code userName!clusterId!flowId!flowRunId!AppId!entityType!}
   *
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @param appId
   * @param entityType
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKeyPrefix(String clusterId, String userId,
      String flowId, Long flowRunId, String appId, String entityType) {
    byte[] first =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(userId, clusterId,
            flowId));
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(EntityRowKey.invert(flowRunId));
    byte[] third =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(appId, entityType, ""));
    return Separator.QUALIFIERS.join(first, second, third);
  }

  /**
   * Constructs a row key for the entity table as follows:
   * {@code userName!clusterId!flowId!flowRunId!AppId!entityType!entityId}
   *
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @param appId
   * @param entityType
   * @param entityId
   * @return byte array with the row key
   */
  public static byte[] getRowKey(String clusterId, String userId,
      String flowId, Long flowRunId, String appId, String entityType,
      String entityId) {
    byte[] first =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(userId, clusterId,
            flowId));
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(EntityRowKey.invert(flowRunId));
    byte[] third =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(appId, entityType,
            entityId));
    return Separator.QUALIFIERS.join(first, second, third);
  }

  /**
   * Constructs an application row key prefix as follows:
   * {@code userName!clusterId!flowId!flowRunId!AppId}
   * 
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @param appId
   * @return
   */
  public static byte[] getRowKey(String clusterId, String userId,
      String flowId, Long flowRunId, String appId) {
    return getRowKeyPrefix(clusterId, userId, flowId, flowRunId, appId);
  }

  /**
   * Converts a timestamp into it's inverse timestamp to be used in (row) keys
   * where we want to have the most recent timestamp in the top of the table
   * (scans start at the most recent timestamp first).
   *
   * @param key value to be inverted so that the latest version will be first in
   *          a scan.
   * @return inverted long
   */
  public static long invert(Long key) {
    return Long.MAX_VALUE - key;
  }

}
