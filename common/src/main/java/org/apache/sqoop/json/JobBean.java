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
import static org.apache.sqoop.json.util.ConfigInputSerialization.restoreConfigs;
import static org.apache.sqoop.json.util.ConfigInputSerialization.restoreValidator;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.json.util.ConfigInputConstants;
import org.apache.sqoop.json.util.ConfigValidatorConstants;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.model.MValidator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Json representation of the job
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobBean implements JsonBean {

  static final String FROM_LINK_NAME = "from-link-name";
  static final String TO_LINK_NAME = "to-link-name";
  static final String FROM_CONNECTOR_NAME = "from-connector-name";
  static final String TO_CONNECTOR_NAME = "to-connector-name";
  static final String FROM_CONFIG_VALUES = "from-config-values";
  static final String TO_CONFIG_VALUES = "to-config-values";
  static final String DRIVER_CONFIG_VALUES = "driver-config-values";
  private static final String JOBS = "jobs";

  // Required
  private List<MJob> jobs;

  // Optional
  private Map<String, ResourceBundle> connectorConfigBundles;
  private ResourceBundle driverConfigBundle;

  // For "extract"
  public JobBean(MJob job) {
    this();
    this.jobs = new ArrayList<>();
    this.jobs.add(job);
  }

  public JobBean(List<MJob> jobs) {
    this();
    this.jobs = jobs;
  }

  // For "restore"
  public JobBean() {
    connectorConfigBundles = new HashMap<>();
  }

  public void setDriverConfigBundle(ResourceBundle driverConfigBundle) {
    this.driverConfigBundle = driverConfigBundle;
  }

  public void addConnectorConfigBundle(String connectorName, ResourceBundle connectorConfigBundle) {
    connectorConfigBundles.put(connectorName, connectorConfigBundle);
  }

  public boolean hasConnectorConfigBundle(String connectorName) {
    return connectorConfigBundles.containsKey(connectorName);
  }

  public List<MJob> getJobs() {
    return jobs;
  }

  public ResourceBundle getConnectorConfigBundle(String connectorName) {
    return connectorConfigBundles.get(connectorName);
  }

  public ResourceBundle getDriverConfigBundle() {
    return driverConfigBundle;
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONArray jobArray = extractJobs(skipSensitive);
    JSONObject jobs = new JSONObject();
    jobs.put(JOBS, jobArray);
    return jobs;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void restore(JSONObject jsonObject) {
    JSONArray array = JSONUtils.getJSONArray(jsonObject, JOBS);
    restoreJobs(array);
  }

  protected JSONArray extractJobs(boolean skipSensitive) {
    JSONArray jobArray = new JSONArray();
    for (MJob job : jobs) {
      jobArray.add(extractJob(skipSensitive, job));
    }
    return jobArray;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractJob(boolean skipSensitive, MJob job) {
    JSONObject object = new JSONObject();
    object.put(ID, job.getPersistenceId());
    object.put(NAME, job.getName());
    object.put(ENABLED, job.getEnabled());
    object.put(CREATION_USER, job.getCreationUser());
    object.put(CREATION_DATE, job.getCreationDate().getTime());
    object.put(UPDATE_USER, job.getLastUpdateUser());
    object.put(UPDATE_DATE, job.getLastUpdateDate().getTime());
    // job link associated connectors
    // TODO(SQOOP-1634): fix not to require the connectorIds in the post data
    object.put(FROM_CONNECTOR_NAME, job.getFromConnectorName());
    object.put(TO_CONNECTOR_NAME, job.getToConnectorName());
    // job associated links
    object.put(FROM_LINK_NAME, job.getFromLinkName());
    object.put(TO_LINK_NAME, job.getToLinkName());
    // job configs
    MFromConfig fromConfigList = job.getFromJobConfig();
    object.put(FROM_CONFIG_VALUES, extractConfigList(fromConfigList, skipSensitive));
    MToConfig toConfigList = job.getToJobConfig();
    object.put(TO_CONFIG_VALUES, extractConfigList(toConfigList, skipSensitive));
    MDriverConfig driverConfigList = job.getDriverConfig();
    object.put(
        DRIVER_CONFIG_VALUES,
        extractConfigList(driverConfigList, skipSensitive));

    return object;
  }

  protected void restoreJobs(JSONArray array) {
    jobs = new ArrayList<>();
    for (Object obj : array) {
      jobs.add(restoreJob(obj));
    }
  }

  private MJob restoreJob(Object obj) {
    JSONObject object = (JSONObject) obj;
    String fromConnectorName = JSONUtils.getString(object, FROM_CONNECTOR_NAME);
    String toConnectorName = JSONUtils.getString(object, TO_CONNECTOR_NAME);
    String fromLinkName = JSONUtils.getString(object, FROM_LINK_NAME);
    String toLinkName = JSONUtils.getString(object, TO_LINK_NAME);
    JSONObject fromConfigJson = JSONUtils.getJSONObject(object, FROM_CONFIG_VALUES);
    JSONObject toConfigJson = JSONUtils.getJSONObject(object, TO_CONFIG_VALUES);
    JSONObject driverConfigJson = JSONUtils.getJSONObject(object, DRIVER_CONFIG_VALUES);

    List<MConfig> fromConfigs = restoreConfigs(JSONUtils.getJSONArray(fromConfigJson, ConfigInputConstants.CONFIGS));
    List<MValidator> fromValidators = restoreValidator(JSONUtils.getJSONArray(fromConfigJson, ConfigInputConstants.CONFIG_VALIDATORS));

    List<MConfig> toConfigs = restoreConfigs(JSONUtils.getJSONArray(toConfigJson, ConfigInputConstants.CONFIGS));
    List<MValidator> toValidators = restoreValidator(JSONUtils.getJSONArray(toConfigJson, ConfigInputConstants.CONFIG_VALIDATORS));

    List<MConfig> driverConfigs = restoreConfigs(JSONUtils.getJSONArray(driverConfigJson, ConfigInputConstants.CONFIGS));
    List<MValidator> driverValidators = restoreValidator(JSONUtils.getJSONArray(driverConfigJson, ConfigInputConstants.CONFIG_VALIDATORS));

    MJob job = new MJob(
      fromConnectorName,
      toConnectorName,
      fromLinkName,
      toLinkName,
      new MFromConfig(fromConfigs, fromValidators),
      new MToConfig(toConfigs, toValidators),
      new MDriverConfig(driverConfigs, driverValidators)
    );

    job.setPersistenceId(JSONUtils.getLong(object, ID));
    job.setName(JSONUtils.getString(object, NAME));
    job.setEnabled(JSONUtils.getBoolean(object, ENABLED));
    job.setCreationUser( JSONUtils.getString(object, CREATION_USER));
    job.setCreationDate(new Date(JSONUtils.getLong(object, CREATION_DATE)));
    job.setLastUpdateUser(JSONUtils.getString(object, UPDATE_USER));
    job.setLastUpdateDate(new Date(JSONUtils.getLong(object, UPDATE_DATE)));
    return job;
  }
}
