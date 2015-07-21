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

import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PhoenixRelatedTest extends BaseTest {
  protected static PhoenixAggregatorStorage aggregatorStorage;
  public static final int BATCH_SIZE = 3;


  protected static PhoenixAggregatorStorage setupPhoenixClusterAndWriterForTest(
      YarnConfiguration conf) throws Exception {
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

  protected void verifySQLWithCount(String sql, int targetCount, String message)
      throws Exception {
    try (
        Statement stmt =
          aggregatorStorage.getConnection().createStatement();
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
