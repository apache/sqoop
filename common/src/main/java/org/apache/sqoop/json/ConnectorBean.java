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
package org.apache.sqoop.json;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ConnectorBean implements JsonBean {

  public static final String IDS = "ids";
  public static final String NAMES = "names";
  public static final String CLASSES = "classes";

  private long[] ids;
  private String[] names;
  private String[] classes;

  // for "extract"
  public ConnectorBean(long[] ids, String[] names, String[] classes) {
    this.ids = new long[ids.length];
    System.arraycopy(ids, 0, this.ids, 0, ids.length);
    this.names = new String[names.length];
    System.arraycopy(names, 0, this.names, 0, names.length);
    this.classes = new String[classes.length];
    System.arraycopy(classes, 0, this.classes, 0, classes.length);
  }

  // for "restore"
  public ConnectorBean() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract() {
    JSONObject result = new JSONObject();
    JSONArray idsArray = new JSONArray();
    for (long id : ids) {
      idsArray.add(id);
    }
    result.put(IDS, idsArray);
    JSONArray namesArray = new JSONArray();
    for (String name : names) {
      namesArray.add(name);
    }
    result.put(NAMES, namesArray);
    JSONArray classesArray = new JSONArray();
    for (String clz : classes) {
      classesArray.add(clz);
    }
    result.put(CLASSES, classesArray);
    return result;
  }

  @Override
  public void restore(JSONObject jsonObject) {
    JSONArray idsArray = (JSONArray) jsonObject.get(IDS);
    int idsSize = idsArray.size();
    ids = new long[idsSize];
    for (int i = 0; i<idsSize; i++) {
      ids[i] = (Long) idsArray.get(i);
    }
    JSONArray namesArray = (JSONArray) jsonObject.get(NAMES);
    int namesSize = namesArray.size();
    names = new String[namesSize];
    for (int i = 0; i<namesSize; i++) {
      names[i] = (String) namesArray.get(i);
    }
    JSONArray classesArray = (JSONArray) jsonObject.get(CLASSES);
    int classeSize = classesArray.size();
    classes = new String[classeSize];
    for (int i = 0; i<classeSize; i++) {
      classes[i] = (String) classesArray.get(i);
    }
  }

  public long[] getIds() {
    return this.ids;
  }

  public String[] getNames() {
    return this.names;
  }

  public String[] getClasses() {
    return this.classes;
  }

}
