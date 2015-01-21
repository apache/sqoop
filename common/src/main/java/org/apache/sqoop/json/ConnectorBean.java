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

import static org.apache.sqoop.json.util.ConfigInputSerialization.extractConfigList;
import static org.apache.sqoop.json.util.ConfigInputSerialization.restoreConfigList;
import static org.apache.sqoop.json.util.ConfigBundleSerialization.extractConfigParamBundle;
import static org.apache.sqoop.json.util.ConfigBundleSerialization.restoreConfigParamBundle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
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
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ConnectorBean extends ConfigurableBean {

  // to represent the config and inputs with values
  public static final String CONNECTOR_LINK_CONFIG = "link-config";
  public static final String CONNECTOR_JOB_CONFIG = "job-config";
  private static final String CONNECTOR = "connector";

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
    JSONObject connector = new JSONObject();
    connector.put(CONNECTOR, extractConnector(skipSensitive, connectors.get(0)));
    return connector;
  }

  @SuppressWarnings("unchecked")
  protected JSONArray extractConnectors(boolean skipSensitive) {
    JSONArray connectorArray = new JSONArray();
    for (MConnector connector : connectors) {
      connectorArray.add(extractConnector(skipSensitive, connector));
    }
    return connectorArray;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractConnector(boolean skipSensitive, MConnector connector) {
    JSONObject connectorJsonObject = new JSONObject();
    connectorJsonObject.put(ID, connector.getPersistenceId());
    connectorJsonObject.put(NAME, connector.getUniqueName());
    connectorJsonObject.put(CLASS, connector.getClassName());
    connectorJsonObject.put(CONFIGURABLE_VERSION, connector.getVersion());
    connectorJsonObject.put(
        CONNECTOR_LINK_CONFIG,
        extractConfigList(connector.getLinkConfig().getConfigs(), connector.getLinkConfig()
            .getType(), skipSensitive));

    connectorJsonObject.put(CONNECTOR_JOB_CONFIG, new JSONObject());
    // add sub fields to the job config for from and to
    if (connector.getFromConfig() != null) {
      ((JSONObject) connectorJsonObject.get(CONNECTOR_JOB_CONFIG)).put(
          Direction.FROM,
          extractConfigList(connector.getFromConfig().getConfigs(), connector.getFromConfig()
              .getType(), skipSensitive));
    }
    if (connector.getToConfig() != null) {
      ((JSONObject) connectorJsonObject.get(CONNECTOR_JOB_CONFIG)).put(
          Direction.TO,
          extractConfigList(connector.getToConfig().getConfigs(), connector.getToConfig()
              .getType(), skipSensitive));
    }
    // add the config-param inside each connector
    connectorJsonObject.put(ALL_CONFIGS, new JSONObject());
    if (connectorConfigBundles != null && !connectorConfigBundles.isEmpty()) {
      connectorJsonObject.put(ALL_CONFIGS,
          extractConfigParamBundle(connectorConfigBundles.get(connector.getPersistenceId())));
    }
    return connectorJsonObject;
  }

  @Override
  public void restore(JSONObject jsonObject) {
    connectors = new ArrayList<MConnector>();
    connectorConfigBundles = new HashMap<Long, ResourceBundle>();
    JSONObject obj = (JSONObject) jsonObject.get(CONNECTOR);
    connectors.add(restoreConnector(obj));
  }

  protected void restoreConnectors(JSONArray array) {
    connectors = new ArrayList<MConnector>();
    connectorConfigBundles = new HashMap<Long, ResourceBundle>();
    for (Object obj : array) {
      connectors.add(restoreConnector(obj));
    }
  }

  private MConnector restoreConnector(Object obj) {
    JSONObject object = (JSONObject) obj;
    long connectorId = (Long) object.get(ID);
    String uniqueName = (String) object.get(NAME);
    String className = (String) object.get(CLASS);
    String version = (String) object.get(CONFIGURABLE_VERSION);

    List<MConfig> linkConfigs = restoreConfigList((JSONArray) object
        .get(CONNECTOR_LINK_CONFIG));

    // parent that encapsulates both the from/to configs
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
    if (object.containsKey(ALL_CONFIGS)) {

      JSONObject jsonConfigBundle = (JSONObject) object.get(ALL_CONFIGS);
      connectorConfigBundles.put(connectorId, restoreConfigParamBundle(jsonConfigBundle));
    }
    return connector;
  }
}
