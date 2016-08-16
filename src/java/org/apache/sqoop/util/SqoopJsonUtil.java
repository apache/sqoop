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

package org.apache.sqoop.util;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.json.JSONObject;

public class SqoopJsonUtil {

  public static final Log LOG = LogFactory
    .getLog(SqoopJsonUtil.class.getName());

  private SqoopJsonUtil() {
  }

  public static String getJsonStringforMap(Map<String, String> map) {
    JSONObject pathPartMap = new JSONObject(map);
    return pathPartMap.toString();
  }

  public static Map<String, String> getMapforJsonString(String mapJsonStr) {
    LOG.debug("Passed mapJsonStr ::" + mapJsonStr + " to parse");
    final Map<String, String> result;
    try {
      if (isEmptyJSON(mapJsonStr)) {
        result = Collections.emptyMap();
      } else {
        ObjectMapper mapper = new ObjectMapper();
        result = mapper.readValue(mapJsonStr,
        new TypeReference<HashMap<String, String>>() {
        });
      }
      return result;
    } catch (JsonParseException e) {
      LOG.error("JsonParseException:: Illegal json to parse into map :"
        + mapJsonStr + e.getMessage());
      throw new IllegalArgumentException("Illegal json to parse into map :"
        + mapJsonStr + e.getMessage(), e);
    } catch (JsonMappingException e) {
      LOG.error("JsonMappingException:: Illegal json to parse into map :"
        + mapJsonStr + e.getMessage());
      throw new IllegalArgumentException("Illegal json to parse into map :"
        + mapJsonStr + e.getMessage(), e);
    } catch (IOException e) {
      LOG.error("IOException while parsing json into map :" + mapJsonStr
        + e.getMessage());
      throw new IllegalArgumentException(
        "IOException while parsing json into map :" + mapJsonStr
          + e.getMessage(), e);
    }
  }

  public static boolean isEmptyJSON(String jsonStr) {
    if (null == jsonStr || "".equals(jsonStr) || "{}".equals(jsonStr)) {
      return true;
    } else {
      return false;
    }
  }
}