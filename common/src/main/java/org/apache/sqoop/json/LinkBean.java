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
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MConfig;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

import static org.apache.sqoop.json.util.ConfigSerialization.*;
import static org.apache.sqoop.json.util.ResourceBundleSerialization.*;

/**
 * Link representation that is being send across the network between
 * Sqoop server and client. Server might optionally send configs
 * associated with the links to spare client of sending another HTTP
 * requests to obtain them.
 */
public class LinkBean implements JsonBean {

  static final String CONNECTOR_ID = "connector-id";
  static final String LINK_CONFIG = "link-config";

  // Required
  private List<MLink> links;

  // Optional
  private Map<Long, ResourceBundle> linkConfigBundles;

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
    linkConfigBundles = new HashMap<Long, ResourceBundle>();
  }

  public void addConnectorConfigBundle(Long id, ResourceBundle connectorConfigBundle) {
    linkConfigBundles.put(id, connectorConfigBundle);
  }

  public boolean hasConnectorConfigBundle(Long id) {
    return linkConfigBundles.containsKey(id);
  }

  public List<MLink> getLinks() {
    return links;
  }

  public ResourceBundle getConnectorConfigBundle(Long id) {
    return linkConfigBundles.get(id);
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONArray linkArray = new JSONArray();

    for(MLink link : links) {
      JSONObject linkJsonObject = new JSONObject();

      linkJsonObject.put(ID, link.getPersistenceId());
      linkJsonObject.put(NAME, link.getName());
      linkJsonObject.put(ENABLED, link.getEnabled());
      linkJsonObject.put(CREATION_USER, link.getCreationUser());
      linkJsonObject.put(CREATION_DATE, link.getCreationDate().getTime());
      linkJsonObject.put(UPDATE_USER, link.getLastUpdateUser());
      linkJsonObject.put(UPDATE_DATE, link.getLastUpdateDate().getTime());
      linkJsonObject.put(CONNECTOR_ID, link.getConnectorId());
      linkJsonObject.put(LINK_CONFIG,
        extractConfigList(link.getConnectorLinkConfig().getConfigs(), skipSensitive));

      linkArray.add(linkJsonObject);
    }

    JSONObject all = new JSONObject();
    all.put(ALL, linkArray);
    if (!linkConfigBundles.isEmpty()) {
      JSONObject bundles = new JSONObject();
      for (Map.Entry<Long, ResourceBundle> entry : linkConfigBundles.entrySet()) {
        bundles.put(entry.getKey().toString(), extractResourceBundle(entry.getValue()));
      }
      all.put(CONNECTOR_CONFIGS, bundles);
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
      JSONArray connectorLinkConfig = (JSONArray) object.get(LINK_CONFIG);

      List<MConfig> linkConfig = restoreConfigList(connectorLinkConfig);

      MLink link = new MLink(connectorId, new MLinkConfig(linkConfig));

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
        linkConfigBundles.put(Long.parseLong(entry.getKey()),
                             restoreResourceBundle(entry.getValue()));
      }
    }
  }
}
