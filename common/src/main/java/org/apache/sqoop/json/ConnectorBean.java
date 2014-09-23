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

import static org.apache.sqoop.json.util.FormSerialization.ALL;
import static org.apache.sqoop.json.util.FormSerialization.CLASS;
import static org.apache.sqoop.json.util.FormSerialization.CON_FORMS;
import static org.apache.sqoop.json.util.FormSerialization.ID;
import static org.apache.sqoop.json.util.FormSerialization.JOB_FORMS;
import static org.apache.sqoop.json.util.FormSerialization.NAME;
import static org.apache.sqoop.json.util.FormSerialization.VERSION;
import static org.apache.sqoop.json.util.FormSerialization.extractForms;
import static org.apache.sqoop.json.util.FormSerialization.restoreForms;
import static org.apache.sqoop.json.util.ResourceBundleSerialization.CONNECTOR_CONFIGS;
import static org.apache.sqoop.json.util.ResourceBundleSerialization.extractResourceBundle;
import static org.apache.sqoop.json.util.ResourceBundleSerialization.restoreResourceBundle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MJobForms;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ConnectorBean implements JsonBean {

  private List<MConnector> connectors;

  private Map<Long, ResourceBundle> bundles;

  // for "extract"
  public ConnectorBean(List<MConnector> connectors,
                       Map<Long, ResourceBundle> bundles) {
    this.connectors = connectors;
    this.bundles = bundles;
  }

  // for "restore"
  public ConnectorBean() {
  }

  public List<MConnector> getConnectors() {
    return connectors;
  }

  public Map<Long, ResourceBundle> getResourceBundles() {
    return bundles;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract(boolean skipSensitive) {

    JSONArray array = new JSONArray();

    for (MConnector connector : connectors) {
      JSONObject object = new JSONObject();

      object.put(ID, connector.getPersistenceId());
      object.put(NAME, connector.getUniqueName());
      object.put(CLASS, connector.getClassName());
      object.put(VERSION, connector.getVersion());

      object.put(CON_FORMS, extractForms(connector.getConnectionForms().getForms(), skipSensitive));
      object.put(JOB_FORMS, new JSONObject());
      ((JSONObject)object.get(JOB_FORMS)).put(
          Direction.FROM, extractForms(connector.getJobForms(Direction.FROM).getForms(), skipSensitive));
      ((JSONObject)object.get(JOB_FORMS)).put(
          Direction.TO, extractForms(connector.getJobForms(Direction.TO).getForms(), skipSensitive));
      array.add(object);
    }

    JSONObject all = new JSONObject();
    all.put(ALL, array);

    if(bundles != null && !bundles.isEmpty()) {
      JSONObject jsonBundles = new JSONObject();

      for(Map.Entry<Long, ResourceBundle> entry : bundles.entrySet()) {
        jsonBundles.put(entry.getKey().toString(),
                         extractResourceBundle(entry.getValue()));
      }
      all.put(CONNECTOR_CONFIGS, jsonBundles);
    }

    return all;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void restore(JSONObject jsonObject) {
    connectors = new ArrayList<MConnector>();

    JSONArray array = (JSONArray) jsonObject.get(ALL);

    for (Object obj : array) {
      JSONObject object = (JSONObject) obj;

      long connectorId = (Long) object.get(ID);
      String uniqueName = (String) object.get(NAME);
      String className = (String) object.get(CLASS);
      String version = (String) object.get(VERSION);

      List<MForm> connForms = restoreForms((JSONArray) object.get(CON_FORMS));
      JSONObject jobJson = (JSONObject) object.get(JOB_FORMS);
      JSONArray fromJobJson = (JSONArray)jobJson.get(Direction.FROM.name());
      JSONArray toJobJson = (JSONArray)jobJson.get(Direction.TO.name());
      List<MForm> fromJobForms = restoreForms(fromJobJson);
      List<MForm> toJobForms = restoreForms(toJobJson);
      MJobForms fromJob = new MJobForms(fromJobForms);
      MJobForms toJob = new MJobForms(toJobForms);
      MConnectionForms connection = new MConnectionForms(connForms);
      MConnector connector = new MConnector(uniqueName, className, version, connection, fromJob,
          toJob);
      connector.setPersistenceId(connectorId);
      connectors.add(connector);
    }

    if(jsonObject.containsKey(CONNECTOR_CONFIGS)) {
      bundles = new HashMap<Long, ResourceBundle>();

      JSONObject jsonBundles = (JSONObject) jsonObject.get(CONNECTOR_CONFIGS);
      Set<Map.Entry<String, JSONObject>> entrySet = jsonBundles.entrySet();
      for (Map.Entry<String, JSONObject> entry : entrySet) {
        bundles.put(Long.parseLong(entry.getKey()),
                             restoreResourceBundle(entry.getValue()));
      }
    }
  }
}
