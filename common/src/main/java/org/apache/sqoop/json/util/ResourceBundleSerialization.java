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
package org.apache.sqoop.json.util;

import org.apache.sqoop.utils.MapResourceBundle;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

/**
 *
 */
public final class ResourceBundleSerialization {

  public static final String RESOURCES = "resources";
  public static final String CONNECTOR_RESOURCES = "resources-connector";
  public static final String FRAMEWORK_RESOURCES = "resources-framework";

  @SuppressWarnings("unchecked")
  public static JSONArray extractResourceBundles(List<ResourceBundle> bundles) {
    JSONArray array = new JSONArray();
    for (ResourceBundle bundle : bundles) {
      array.add(extractResourceBundle(bundle));
    }

    return array;
  }

  @SuppressWarnings("unchecked")
  public static JSONObject extractResourceBundle(ResourceBundle bundle) {
    JSONObject json = new JSONObject();

    Enumeration<String> keys = bundle.getKeys();

    while(keys.hasMoreElements()) {
      String key = keys.nextElement();

      json.put(key, bundle.getString(key));

    }

    return json;
  }

  @SuppressWarnings("unchecked")
  public static List<ResourceBundle> restoreResourceBundles(JSONArray array) {
    List<ResourceBundle> bundles = new LinkedList<ResourceBundle>();

    for (Object item : array) {
      bundles.add(restoreResourceBundle((JSONObject) item));
    }

    return bundles;
  }

  @SuppressWarnings("unchecked")
  public static ResourceBundle restoreResourceBundle(JSONObject json) {
    Map<String, Object> map = new HashMap<String, Object>();
    map.putAll(json);
    return new MapResourceBundle(map);
  }

  private ResourceBundleSerialization() {
    // Instantiation of this class is prohibited
  }
}
