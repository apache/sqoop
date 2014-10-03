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

import static org.apache.sqoop.json.util.ConfigSerialization.DRIVER_CONFIG;
import static org.apache.sqoop.json.util.ConfigSerialization.DRIVER_VERSION;
import static org.apache.sqoop.json.util.ConfigSerialization.ID;
import static org.apache.sqoop.json.util.ConfigSerialization.extractConfigList;
import static org.apache.sqoop.json.util.ConfigSerialization.restoreConfigList;
import static org.apache.sqoop.json.util.ResourceBundleSerialization.CONFIGS;
import static org.apache.sqoop.json.util.ResourceBundleSerialization.extractResourceBundle;
import static org.apache.sqoop.json.util.ResourceBundleSerialization.restoreResourceBundle;

import java.util.List;
import java.util.ResourceBundle;

import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MDriverConfig;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
/**
 * Json representation of the driver
 *
 */
public class DriverBean implements JsonBean {

  public static final String CURRENT_DRIVER_VERSION = "1";

  private MDriver driver;

  private ResourceBundle bundle;

  // for "extract"
  public DriverBean(MDriver driver, ResourceBundle bundle) {
    this.driver = driver;
    this.bundle = bundle;
  }

  // for "restore"
  public DriverBean() {
  }

  public MDriver getDriver() {
    return driver;
  }

  public ResourceBundle getDriverConfigResourceBundle() {
    return bundle;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract(boolean skipSensitive) {
    JSONArray configs =
      extractConfigList(driver.getDriverConfig().getConfigs(), skipSensitive);

    JSONObject result = new JSONObject();
    result.put(ID, driver.getPersistenceId());
    result.put(DRIVER_VERSION, driver.getVersion());
    result.put(DRIVER_CONFIG, configs);
    result.put(CONFIGS, extractResourceBundle(bundle));
    return result;
  }

  @Override
  public void restore(JSONObject jsonObject) {
    long id = (Long) jsonObject.get(ID);
    String driverVersion = (String) jsonObject.get(DRIVER_VERSION);
    List<MConfig> driverConfig = restoreConfigList((JSONArray) jsonObject.get(DRIVER_CONFIG));
    driver = new MDriver(new MDriverConfig(driverConfig), driverVersion);
    driver.setPersistenceId(id);
    bundle = restoreResourceBundle((JSONObject) jsonObject.get(CONFIGS));
  }
}
