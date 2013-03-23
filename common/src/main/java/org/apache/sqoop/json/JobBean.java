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
  private static final String TYPE = "type";
  private static final String CONNECTION_ID = "connection-id";
  private static final String CONNECTOR_ID = "connector-id";
  private static final String CONNECTOR_PART = "connector";
  private static final String FRAMEWORK_PART = "framework";

  // Compulsory
  private List<MJob> jobs;

  // Optional
  private Map<Long, ResourceBundle> connectorBundles;
  private ResourceBundle frameworkBundle;

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
    connectorBundles = new HashMap<Long, ResourceBundle>();
  }

  public void setFrameworkBundle(ResourceBundle frameworkBundle) {
    this.frameworkBundle = frameworkBundle;
  }

  public void addConnectorBundle(Long id, ResourceBundle connectorBundle) {
    connectorBundles.put(id, connectorBundle);
  }

  public boolean hasConnectorBundle(Long id) {
    return connectorBundles.containsKey(id);
  }

  public List<MJob> getJobs() {
    return jobs;
  }

  public ResourceBundle getConnectorBundle(Long id) {
    return connectorBundles.get(id);
  }

  public ResourceBundle getFrameworkBundle() {
    return frameworkBundle;
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONArray array = new JSONArray();

    for(MJob job : jobs) {
      JSONObject object = new JSONObject();

      object.put(ID, job.getPersistenceId());
      object.put(NAME, job.getName());
      object.put(TYPE, job.getType().name());
      object.put(CREATED, job.getCreationDate().getTime());
      object.put(UPDATED, job.getLastUpdateDate().getTime());
      object.put(CONNECTION_ID, job.getConnectionId());
      object.put(CONNECTOR_ID, job.getConnectorId());
      object.put(CONNECTOR_PART,
        extractForms(job.getConnectorPart().getForms(), skipSensitive));
      object.put(FRAMEWORK_PART,
        extractForms(job.getFrameworkPart().getForms(), skipSensitive));

      array.add(object);
    }

    JSONObject all = new JSONObject();
    all.put(ALL, array);

    if(!connectorBundles.isEmpty()) {
      JSONObject bundles = new JSONObject();

      for(Map.Entry<Long, ResourceBundle> entry : connectorBundles.entrySet()) {
        bundles.put(entry.getKey().toString(),
                    extractResourceBundle(entry.getValue()));
      }

      all.put(CONNECTOR_RESOURCES, bundles);
    }
    if(frameworkBundle != null) {
      all.put(FRAMEWORK_RESOURCES,extractResourceBundle(frameworkBundle));
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

      long connectorId = (Long) object.get(CONNECTOR_ID);
      long connectionId = (Long) object.get(CONNECTION_ID);
      JSONArray connectorPart = (JSONArray) object.get(CONNECTOR_PART);
      JSONArray frameworkPart = (JSONArray) object.get(FRAMEWORK_PART);

      String stringType = (String) object.get(TYPE);
      MJob.Type type = MJob.Type.valueOf(stringType);

      List<MForm> connectorForms = restoreForms(connectorPart);
      List<MForm> frameworkForms = restoreForms(frameworkPart);

      MJob job = new MJob(
        connectorId,
        connectionId,
        type,
        new MJobForms(type, connectorForms),
        new MJobForms(type, frameworkForms)
      );

      job.setPersistenceId((Long) object.get(ID));
      job.setName((String) object.get(NAME));
      job.setCreationDate(new Date((Long) object.get(CREATED)));
      job.setLastUpdateDate(new Date((Long) object.get(UPDATED)));

      jobs.add(job);
    }

    if(jsonObject.containsKey(CONNECTOR_RESOURCES)) {
      JSONObject bundles = (JSONObject) jsonObject.get(CONNECTOR_RESOURCES);
      Set<Map.Entry<String, JSONObject>> entrySet = bundles.entrySet();
      for (Map.Entry<String, JSONObject> entry : entrySet) {
        connectorBundles.put(Long.parseLong(entry.getKey()),
                             restoreResourceBundle(entry.getValue()));
      }
    }
    if(jsonObject.containsKey(FRAMEWORK_RESOURCES)) {
      frameworkBundle = restoreResourceBundle(
        (JSONObject) jsonObject.get(FRAMEWORK_RESOURCES));
    }
  }
}
