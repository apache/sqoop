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

import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MForm;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

import static org.apache.sqoop.json.util.FormSerialization.*;
import static org.apache.sqoop.json.util.ResourceBundleSerialization.*;

/**
 * Link representation that is being send across the network between
 * Sqoop server and client. Server might optionally send configs
 * associated with the links to spare client of sending another HTTP
 * requests to obtain them.
 */
public class LinkBean implements JsonBean {

  private static final String CONNECTOR_ID = "connector-id";
  private static final String CONNECTOR_PART = "connector";
  private static final String FRAMEWORK_PART = "framework";

  // Required
  private List<MLink> links;

  // Optional
  private Map<Long, ResourceBundle> connectorConfigBundles;
  private ResourceBundle driverConfigBundle;

  // For "extract"
  public LinkBean(MLink link) {
    this();
    this.links = new ArrayList<MLink>();
    this.links.add(link);
  }

  public LinkBean(List<MLink> links) {
    this();
    this.links = links;
  }

  // For "restore"
  public LinkBean() {
    connectorConfigBundles = new HashMap<Long, ResourceBundle>();
  }

  public void setDriverConfigBundle(ResourceBundle driverConfigBundle) {
    this.driverConfigBundle = driverConfigBundle;
  }

  public void addConnectorConfigBundle(Long id, ResourceBundle connectorConfigBundle) {
    connectorConfigBundles.put(id, connectorConfigBundle);
  }

  public boolean hasConnectorBundle(Long id) {
    return connectorConfigBundles.containsKey(id);
  }

  public List<MLink> getLinks() {
    return links;
  }

  public ResourceBundle getConnectorBundle(Long id) {
    return connectorConfigBundles.get(id);
  }

  public ResourceBundle getFrameworkBundle() {
    return driverConfigBundle;
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONArray array = new JSONArray();

    for(MLink link : links) {
      JSONObject object = new JSONObject();

      object.put(ID, link.getPersistenceId());
      object.put(NAME, link.getName());
      object.put(ENABLED, link.getEnabled());
      object.put(CREATION_USER, link.getCreationUser());
      object.put(CREATION_DATE, link.getCreationDate().getTime());
      object.put(UPDATE_USER, link.getLastUpdateUser());
      object.put(UPDATE_DATE, link.getLastUpdateDate().getTime());
      object.put(CONNECTOR_ID, link.getConnectorId());
      object.put(CONNECTOR_PART,
        extractForms(link.getConnectorPart().getForms(), skipSensitive));
      object.put(FRAMEWORK_PART,
        extractForms(link.getFrameworkPart().getForms(), skipSensitive));

      array.add(object);
    }

    JSONObject all = new JSONObject();
    all.put(ALL, array);

    if(!connectorConfigBundles.isEmpty()) {
      JSONObject bundles = new JSONObject();

      for(Map.Entry<Long, ResourceBundle> entry : connectorConfigBundles.entrySet()) {
        bundles.put(entry.getKey().toString(),
                    extractResourceBundle(entry.getValue()));
      }
      all.put(CONNECTOR_CONFIGS, bundles);
    }
    if(driverConfigBundle != null) {
      all.put(DRIVER_CONFIGS,extractResourceBundle(driverConfigBundle));
    }
    return all;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void restore(JSONObject jsonObject) {
    links = new ArrayList<MLink>();

    JSONArray array = (JSONArray) jsonObject.get(ALL);

    for (Object obj : array) {
      JSONObject object = (JSONObject) obj;

      long connectorId = (Long) object.get(CONNECTOR_ID);
      JSONArray connectorPart = (JSONArray) object.get(CONNECTOR_PART);
      JSONArray frameworkPart = (JSONArray) object.get(FRAMEWORK_PART);

      List<MForm> connectorForms = restoreForms(connectorPart);
      List<MForm> frameworkForms = restoreForms(frameworkPart);

      MLink link = new MLink(connectorId,
        new MConnectionForms(connectorForms),
        new MConnectionForms(frameworkForms));

      link.setPersistenceId((Long) object.get(ID));
      link.setName((String) object.get(NAME));
      link.setEnabled((Boolean) object.get(ENABLED));
      link.setCreationUser((String) object.get(CREATION_USER));
      link.setCreationDate(new Date((Long) object.get(CREATION_DATE)));
      link.setLastUpdateUser((String) object.get(UPDATE_USER));
      link.setLastUpdateDate(new Date((Long) object.get(UPDATE_DATE)));

      links.add(link);
    }

    if(jsonObject.containsKey(CONNECTOR_CONFIGS)) {
      JSONObject bundles = (JSONObject) jsonObject.get(CONNECTOR_CONFIGS);
      Set<Map.Entry<String, JSONObject>> entrySet = bundles.entrySet();
      for (Map.Entry<String, JSONObject> entry : entrySet) {
        connectorConfigBundles.put(Long.parseLong(entry.getKey()),
                             restoreResourceBundle(entry.getValue()));
      }
    }
    if(jsonObject.containsKey(DRIVER_CONFIGS)) {
      driverConfigBundle = restoreResourceBundle(
        (JSONObject) jsonObject.get(DRIVER_CONFIGS));
    }
  }
}
