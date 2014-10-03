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

import static org.apache.sqoop.json.util.ConfigSerialization.ALL;
import static org.apache.sqoop.json.util.ConfigSerialization.CLASS;
import static org.apache.sqoop.json.util.ConfigSerialization.ID;
import static org.apache.sqoop.json.util.ConfigSerialization.CONNECTOR_JOB_CONFIG;
import static org.apache.sqoop.json.util.ConfigSerialization.CONNECTOR_LINK_CONFIG;
import static org.apache.sqoop.json.util.ConfigSerialization.NAME;
import static org.apache.sqoop.json.util.ConfigSerialization.VERSION;
import static org.apache.sqoop.json.util.ConfigSerialization.extractConfigList;
import static org.apache.sqoop.json.util.ConfigSerialization.restoreConfigList;
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
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MToConfig;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Json representation of the connector object
 *
 */
public class ConnectorBean implements JsonBean {

  private List<MConnector> connectors;

  private Map<Long, ResourceBundle> connectorConfigBundles;

  // for "extract"
  public ConnectorBean(List<MConnector> connectors, Map<Long, ResourceBundle> bundles) {
    this.connectors = connectors;
    this.connectorConfigBundles = bundles;
  }

  // for "restore"
  public ConnectorBean() {
  }

  public List<MConnector> getConnectors() {
    return connectors;
  }

  public Map<Long, ResourceBundle> getResourceBundles() {
    return connectorConfigBundles;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract(boolean skipSensitive) {

    JSONArray connectorArray = new JSONArray();

    for (MConnector connector : connectors) {
      JSONObject connectorJsonObject = new JSONObject();

      connectorJsonObject.put(ID, connector.getPersistenceId());
      connectorJsonObject.put(NAME, connector.getUniqueName());
      connectorJsonObject.put(CLASS, connector.getClassName());
      connectorJsonObject.put(VERSION, connector.getVersion());
      connectorJsonObject.put(CONNECTOR_LINK_CONFIG,
          extractConfigList(connector.getLinkConfig().getConfigs(), skipSensitive));

      connectorJsonObject.put(CONNECTOR_JOB_CONFIG, new JSONObject());
      // add sub fields to the job config for from and to
      if (connector.getFromConfig() != null) {
        ((JSONObject) connectorJsonObject.get(CONNECTOR_JOB_CONFIG)).put(Direction.FROM,
            extractConfigList(connector.getFromConfig().getConfigs(), skipSensitive));
      }
      if (connector.getToConfig() != null) {
        ((JSONObject) connectorJsonObject.get(CONNECTOR_JOB_CONFIG)).put(Direction.TO,
            extractConfigList(connector.getToConfig().getConfigs(), skipSensitive));
      }
      connectorArray.add(connectorJsonObject);
    }

    JSONObject all = new JSONObject();
    all.put(ALL, connectorArray);

    if (connectorConfigBundles != null && !connectorConfigBundles.isEmpty()) {
      JSONObject jsonBundles = new JSONObject();

      for (Map.Entry<Long, ResourceBundle> entry : connectorConfigBundles.entrySet()) {
        jsonBundles.put(entry.getKey().toString(), extractResourceBundle(entry.getValue()));
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

      List<MConfig> linkConfigs = restoreConfigList((JSONArray) object.get(CONNECTOR_LINK_CONFIG));

      // parent that encapsualtes both the from/to configs
      JSONObject jobConfigJson = (JSONObject) object.get(CONNECTOR_JOB_CONFIG);
      JSONArray fromJobConfigJson = (JSONArray) jobConfigJson.get(Direction.FROM.name());
      JSONArray toJobConfigJson = (JSONArray) jobConfigJson.get(Direction.TO.name());

      MFromConfig fromConfig = null;
      MToConfig toConfig = null;
      if (fromJobConfigJson != null) {

        List<MConfig> fromJobConfig = restoreConfigList(fromJobConfigJson);
        fromConfig = new MFromConfig(fromJobConfig);

      }
      if (toJobConfigJson != null) {
        List<MConfig> toJobConfig = restoreConfigList(toJobConfigJson);
        toConfig = new MToConfig(toJobConfig);
      }

      MLinkConfig linkConfig = new MLinkConfig(linkConfigs);
      MConnector connector = new MConnector(uniqueName, className, version, linkConfig, fromConfig,
          toConfig);

      connector.setPersistenceId(connectorId);
      connectors.add(connector);
    }

    if (jsonObject.containsKey(CONNECTOR_CONFIGS)) {
      connectorConfigBundles = new HashMap<Long, ResourceBundle>();

      JSONObject jsonBundles = (JSONObject) jsonObject.get(CONNECTOR_CONFIGS);
      Set<Map.Entry<String, JSONObject>> entrySet = jsonBundles.entrySet();
      for (Map.Entry<String, JSONObject> entry : entrySet) {
        connectorConfigBundles.put(Long.parseLong(entry.getKey()),
            restoreResourceBundle(entry.getValue()));
      }
    }
  }
}
