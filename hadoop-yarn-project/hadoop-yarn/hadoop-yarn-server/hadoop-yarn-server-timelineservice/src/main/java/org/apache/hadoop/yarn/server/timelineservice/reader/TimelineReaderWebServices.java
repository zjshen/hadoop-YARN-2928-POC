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

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineAggregateBasis;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import com.google.inject.Singleton;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


/** REST end point for Timeline Reader */
@Private
@Unstable
@Singleton
@Path("/ws/v2/timeline")
public class TimelineReaderWebServices {


  private static final Log LOG =
      LogFactory.getLog(TimelineReaderWebServices.class);

  private static final Splitter COMMA_SPLITTER =
      Splitter.on(",").omitEmptyStrings().trimResults();
  private static final Splitter COLON_SPLITTER =
      Splitter.on(":").omitEmptyStrings().trimResults();

  private static void init(HttpServletResponse response) {
    response.setContentType(null);
  }

  private static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUGI;
  }

  @Context
  private ServletContext context;


  @Inject
  public TimelineReaderWebServices() {
  }
  /**
   * Return the description of the timeline reader web services.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineAbout about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    init(res);
    return TimelineUtils.createTimelineAbout("Timeline Reader API");
  }

  @GET
  @Path("/entities/{clusterId}/{appId}/{entityType}/{entityId}")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterId") String clusterId,
      @PathParam("appId") String appId,
      @PathParam("entityType") String entityType,
      @PathParam("entityId") String entityId,
      @QueryParam("userId") String userId,
      @QueryParam("flowId") String flowId,
      @QueryParam("flowRunId") Long flowRunId,
      @QueryParam("fields") String fields) throws IOException {
    init(res);
    UserGroupInformation callerUGI = getUser(req);
    TimelineEntity entity = getTimelineReaderManager().getEntity(
        callerUGI != null && (userId == null || userId.isEmpty()) ?
            callerUGI.getUserName() : userId,
        clusterId, flowId, flowRunId, appId, entityType, entityId,
        parseFieldsStr(fields), callerUGI);
    if (entity == null) {
      throw new NotFoundException("Entity is not found for userId=" + userId +
          ", clusterId=" + clusterId + ", appId=" + appId + ", entityType=" +
          entityType + ", entityId=" + entityId);
    } else {
      LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(entity, true));
      return entity;
    }
  }

  @GET
  @Path("/entities/{clusterId}/{appId}/{entityType}")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterId") String clusterId,
      @PathParam("appId") String appId,
      @PathParam("entityType") String entityType,
      @QueryParam("userId") String userId,
      @QueryParam("flowId") String flowId,
      @QueryParam("flowRunId") Long flowRunId,
      @QueryParam("limit") Long limit,
      @QueryParam("createdTimeBegin") Long createdTimeBegin,
      @QueryParam("createdTimeEnd") Long createdTimeEnd,
      @QueryParam("modifiedTimeBegin") Long modifiedTimeBegin,
      @QueryParam("modifiedTimeEnd") Long modifiedTimeEnd,
      @QueryParam("relatesTo") String relatesTo,
      @QueryParam("isRelatedTo") String isRelatedTo,
      @QueryParam("infoFilters") String infoFilters,
      @QueryParam("configFilters") String configFilters,
      @QueryParam("metricFilters") String metricFilters,
      @QueryParam("eventFilters") String eventFilters,
      @QueryParam("fields") String fields) throws IOException {
    init(res);
    UserGroupInformation callerUGI = getUser(req);
    return getTimelineReaderManager().getEntities(
        callerUGI != null && (userId == null || userId.isEmpty()) ?
            callerUGI.getUserName() : userId, clusterId, flowId,
        flowRunId, appId, entityType, limit, createdTimeBegin, createdTimeEnd,
        modifiedTimeBegin, modifiedTimeBegin, parseKeyStrValuesStr(relatesTo),
        parseKeyStrValuesStr(isRelatedTo), parseKeyObjValueStr(infoFilters),
        parseKeyStrValueStr(configFilters), parseValuesStr(metricFilters),
        parseValuesStr(eventFilters), parseFieldsStr(fields), callerUGI);
  }

  @GET
  @Path("/aggregates/{basis}/{clusterId}/{aggregateId}")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getAggregates(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("basis") String basis,
      @PathParam("clusterId") String clusterId,
      @PathParam("aggregateId") String aggEntityId,
      @QueryParam("userId") String userId) throws IOException {
    init(res);
    UserGroupInformation callerUGI = getUser(req);
    switch (TimelineAggregateBasis.valueOf(basis)) {
      case APPLICATION:
        TimelineEntity aggEntity = getTimelineReaderManager().getEntity(userId,
            clusterId, null, null, aggEntityId,
            TimelineEntityType.YARN_APPLICATION_AGGREGATION.toString(),
            aggEntityId, EnumSet.of(Field.METRICS), callerUGI);
        if (aggEntity == null) {
          throw new NotFoundException("Aggregate is not found for basis=" +
              basis + ", clusterId=" + clusterId + ", aggregateId=" +
              aggEntityId + ", userId=" + userId);
        }
        return aggEntity;
      case FLOW:
        return null;
      case USER:
        return null;
      default:
        throw new NotFoundException("Unsupported aggregate basis: " + basis);
    }
  }

  private static EnumSet<Field> parseFieldsStr(String str) {
    EnumSet enums = EnumSet.noneOf(Field.class);
    if (str == null) {
      return enums;
    }
    Iterator<String> fieldStrs = COMMA_SPLITTER.split(str).iterator();
    while (fieldStrs.hasNext()) {
      enums.add(Field.valueOf(fieldStrs.next()));
    }
    return enums;
  }

  private static Map<String, String> parseKeyStrValueStr(String str) {
    Map<String, String> map = new HashMap<>();
    if (str == null) {
      return map;
    }
    Iterator<String> pairs = COMMA_SPLITTER.split(str).iterator();
    while (pairs.hasNext()) {
      String pair = pairs.next();
      Iterator<String> tokens = COLON_SPLITTER.split(pair).iterator();
      map.put(tokens.next(), tokens.next());
    }
    return map;
  }

  private static Map<String, Object> parseKeyObjValueStr(String str) {
    Map<String, Object> map = new HashMap<>();
    if (str == null) {
      return map;
    }
    Iterator<String> pairs = COMMA_SPLITTER.split(str).iterator();
    while (pairs.hasNext()) {
      String pair = pairs.next();
      Iterator<String> tokens = COLON_SPLITTER.split(pair).iterator();
      String key = tokens.next();
      String value = tokens.next();
      try {
        map.put(key, GenericObjectMapper.OBJECT_READER.readValue(value));
      } catch (IOException e) {
        map.put(key, value);
      }
    }
    return map;
  }

  private static Map<String, Set<String>> parseKeyStrValuesStr(String str) {
    Map<String, Set<String>> map = new HashMap<>();
    if (str == null) {
      return map;
    }
    Iterator<String> pairs = COMMA_SPLITTER.split(str).iterator();
    while (pairs.hasNext()) {
      String pair = pairs.next();
      Iterator<String> tokens = COLON_SPLITTER.split(pair).iterator();
      String key = tokens.next();
      String value = tokens.next();
      Set<String> values = map.get(key);
      if (values == null) {
        values = new HashSet<>();
        map.put(key, values);
      }
      values.add(value);
    }
    return map;
  }

  private static Map<String, Set<Object>> parseKeyObjValuesStr(String str) {
    Map<String, Set<Object>> map = new HashMap<>();
    if (str == null) {
      return map;
    }
    Iterator<String> pairs = COMMA_SPLITTER.split(str).iterator();
    while (pairs.hasNext()) {
      String pair = pairs.next();
      Iterator<String> tokens = COLON_SPLITTER.split(pair).iterator();
      String key = tokens.next();
      String value = tokens.next();
      Set<Object> values = map.get(key);
      if (values == null) {
        values = new HashSet<>();
        map.put(key, values);
      }
      values.add(value);
      try {
        values.add(GenericObjectMapper.OBJECT_READER.readValue(value));
      } catch (IOException e) {
        values.add(value);
      }
    }
    return map;
  }

  private static Set<String> parseValuesStr(String str) {
    Set<String> set = new HashSet<>();
    if (str == null) {
      return set;
    }
    Iterator<String> values = COMMA_SPLITTER.split(str).iterator();
    while (values.hasNext()) {
      set.add(values.next());
    }
    return set;
  }

  private TimelineReaderManager getTimelineReaderManager() {
    return (TimelineReaderManager) context.getAttribute(
        TimelineReaderServer.TIMELINE_READER_MANAGER_ATTR);
  }
}