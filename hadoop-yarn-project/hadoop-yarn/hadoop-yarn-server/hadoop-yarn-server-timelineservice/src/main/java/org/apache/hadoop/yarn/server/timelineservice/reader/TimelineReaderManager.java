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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

@Private
@Unstable
public class TimelineReaderManager extends AbstractService {

  private TimelineReader reader;

  public TimelineReaderManager(TimelineReader timelineReader) {
    super(TimelineReaderManager.class.getName());
    this.reader = timelineReader;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    reader.init(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  public TimelineEntity getEntity(String userId, String clusterId, String flowId,
      Long flowRunId, String appId, String entityType, String entityId,
      EnumSet<TimelineReader.Field> fieldsToRetrieve,
      UserGroupInformation ugi) throws IOException {
    // TODO: apply UGI
    return reader.getEntity(userId, clusterId, flowId, flowRunId, appId,
        entityType, entityId, fieldsToRetrieve);
  }

  public Set<TimelineEntity> getEntities(String userId, String clusterId,
      String flowId, Long flowRunId, String appId, String entityType,
      Long limit, Long createdTimeBegin, Long createdTimeEnd,
      Long modifiedTimeBegin, Long modifiedTimeEnd,
      Map<String, Set<String>> relatesTo, Map<String, Set<String>> isRelatedTo,
      Map<String, Object> infoFilters, Map<String, String> configFilters,
      Set<String>  metricFilters, Set<String> eventFilters,
      EnumSet<TimelineReader.Field> fieldsToRetrieve,
      UserGroupInformation ugi) throws IOException {
    // TODO: apply UGI
    return reader.getEntities(userId, clusterId, flowId, flowRunId, appId,
        entityType, limit, createdTimeBegin, createdTimeEnd, modifiedTimeBegin,
        modifiedTimeEnd, relatesTo, isRelatedTo, infoFilters, configFilters,
        metricFilters, eventFilters, fieldsToRetrieve);
  }
}
