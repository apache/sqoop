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

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
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
 *
 */
public class JobBean implements JsonBean {

  private static final String ALL = "all";
  private static final String ID = "id";
  private static final String NAME = "name";
  private static final String FROM_LINK_ID = "from-link-id";
  private static final String TO_LINK_ID = "to-link-id";
  private static final String FROM_CONNECTOR_ID = "from-connector-id";
  private static final String TO_CONNECTOR_ID = "to-connector-id";
  private static final String FROM_CONNECTOR_PART = "from-connector";
  private static final String TO_CONNECTOR_PART = "to-connector";
  private static final String FRAMEWORK_PART = "framework";

  // Required
  private List<MJob> jobs;

  // Optional
  private Map<Long, ResourceBundle> connectorConfigBundles;
  private ResourceBundle driverConfigBundle;

  // For "extract"
  public JobBean(MJob job) {
    this();
    this.jobs = new ArrayList<MJob>();
    this.jobs.add(job);
  }

  public JobBean(List<MJob> jobs) {
    this();
    this.jobs = jobs;
  }

  // For "restore"
  public JobBean() {
    connectorConfigBundles = new HashMap<Long, ResourceBundle>();
  }

  public void setDriverConfigBundle(ResourceBundle driverConfigBundle) {
    this.driverConfigBundle = driverConfigBundle;
  }

  public void addConnectorConfigBundle(Long id, ResourceBundle connectorConfigBundle) {
    connectorConfigBundles.put(id, connectorConfigBundle);
  }

  public boolean hasConnectorConfigBundle(Long id) {
    return connectorConfigBundles.containsKey(id);
  }

  public List<MJob> getJobs() {
    return jobs;
  }

  public ResourceBundle getConnectorConfigBundle(Long id) {
    return connectorConfigBundles.get(id);
  }

  public ResourceBundle getDriverConfigBundle() {
    return driverConfigBundle;
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONArray array = new JSONArray();

    for(MJob job : jobs) {
      JSONObject object = new JSONObject();

      object.put(ID, job.getPersistenceId());
      object.put(NAME, job.getName());
      object.put(ENABLED, job.getEnabled());
      object.put(CREATION_USER, job.getCreationUser());
      object.put(CREATION_DATE, job.getCreationDate().getTime());
      object.put(UPDATE_USER, job.getLastUpdateUser());
      object.put(UPDATE_DATE, job.getLastUpdateDate().getTime());
      object.put(FROM_LINK_ID, job.getLinkId(Direction.FROM));
      object.put(TO_LINK_ID, job.getLinkId(Direction.TO));
      object.put(FROM_CONNECTOR_ID, job.getConnectorId(Direction.FROM));
      object.put(TO_CONNECTOR_ID, job.getConnectorId(Direction.TO));
      object.put(FROM_CONNECTOR_PART,
        extractForms(job.getConnectorPart(Direction.FROM).getForms(),skipSensitive));
      object.put(TO_CONNECTOR_PART,
          extractForms(job.getConnectorPart(Direction.TO).getForms(), skipSensitive));
      object.put(FRAMEWORK_PART,
        extractForms(job.getFrameworkPart().getForms(), skipSensitive));

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
    jobs = new ArrayList<MJob>();

    JSONArray array = (JSONArray) jsonObject.get(ALL);

    for (Object obj : array) {
      JSONObject object = (JSONObject) obj;

      long fromConnectorId = (Long) object.get(FROM_CONNECTOR_ID);
      long toConnectorId = (Long) object.get(TO_CONNECTOR_ID);
      long fromConnectionId = (Long) object.get(FROM_LINK_ID);
      long toConnectionId = (Long) object.get(TO_LINK_ID);
      JSONArray fromConnectorPart = (JSONArray) object.get(FROM_CONNECTOR_PART);
      JSONArray toConnectorPart = (JSONArray) object.get(TO_CONNECTOR_PART);
      JSONArray frameworkPart = (JSONArray) object.get(FRAMEWORK_PART);

      List<MForm> fromConnectorParts = restoreForms(fromConnectorPart);
      List<MForm> toConnectorParts = restoreForms(toConnectorPart);
      List<MForm> frameworkForms = restoreForms(frameworkPart);

      MJob job = new MJob(
        fromConnectorId,
        toConnectorId,
        fromConnectionId,
        toConnectionId,
        new MJobForms(fromConnectorParts),
        new MJobForms(toConnectorParts),
        new MJobForms(frameworkForms)
      );

      job.setPersistenceId((Long) object.get(ID));
      job.setName((String) object.get(NAME));
      job.setEnabled((Boolean) object.get(ENABLED));
      job.setCreationUser((String) object.get(CREATION_USER));
      job.setCreationDate(new Date((Long) object.get(CREATION_DATE)));
      job.setLastUpdateUser((String) object.get(UPDATE_USER));
      job.setLastUpdateDate(new Date((Long) object.get(UPDATE_DATE)));

      jobs.add(job);
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
