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

  static final String FROM_LINK_ID = "from-link-id";
  static final String TO_LINK_ID = "to-link-id";
  static final String FROM_CONNECTOR_ID = "from-connector-id";
  static final String TO_CONNECTOR_ID = "to-connector-id";
  static final String FROM_CONFIG_VALUES = "from-config-values";
  static final String TO_CONFIG_VALUES = "to-config-values";
  static final String DRIVER_CONFIG_VALUES = "driver-config-values";
  private static final String JOB = "job";

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
    JSONObject job = new JSONObject();
    job.put(JOB, extractJob(skipSensitive, jobs.get(0)));
    return job;
  }

  @SuppressWarnings("unchecked")
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
    object.put(FROM_CONNECTOR_ID, job.getFromConnectorId());
    object.put(TO_CONNECTOR_ID, job.getToConnectorId());
    // job associated links
    object.put(FROM_LINK_ID, job.getFromLinkId());
    object.put(TO_LINK_ID, job.getToLinkId());
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

  @Override
  public void restore(JSONObject jsonObject) {
    jobs = new ArrayList<MJob>();
    JSONObject obj = JSONUtils.getJSONObject(jsonObject, JOB);
    jobs.add(restoreJob(obj));
  }

  protected void restoreJobs(JSONArray array) {
    jobs = new ArrayList<MJob>();
    for (Object obj : array) {
      jobs.add(restoreJob(obj));
    }
  }

  private MJob restoreJob(Object obj) {
    JSONObject object = (JSONObject) obj;
    long fromConnectorId = JSONUtils.getLong(object, FROM_CONNECTOR_ID);
    long toConnectorId = JSONUtils.getLong(object, TO_CONNECTOR_ID);
    long fromConnectionId = JSONUtils.getLong(object, FROM_LINK_ID);
    long toConnectionId = JSONUtils.getLong(object, TO_LINK_ID);
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
      fromConnectorId,
      toConnectorId,
      fromConnectionId,
      toConnectionId,
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
