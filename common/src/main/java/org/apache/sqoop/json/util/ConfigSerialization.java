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
package org.apache.sqoop.json.util;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigType;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Convenient static methods for serializing config objects.
 */
public final class ConfigSerialization {

  public static final String ALL = "all";
  public static final String ID = "id";
  public static final String NAME = "name";
  public static final String VERSION = "version";
  public static final String CLASS = "class";
  public static final String ENABLED = "enabled";
  public static final String CREATION_USER = "creation-user";
  public static final String CREATION_DATE = "creation-date";
  public static final String UPDATE_USER = "update-user";
  public static final String UPDATE_DATE = "update-date";
  // TODO(VB): Move these constants to connector bean
  public static final String CONNECTOR_LINK_CONFIG = "link-config";
  public static final String CONNECTOR_JOB_CONFIG = "job-config";
  // TODO:move these configs to driver bean
  public static final String DRIVER_VERSION = "driver-version";
  public static final String DRIVER_CONFIG = "driver-config";

  public static final String CONFIG_NAME = "name";
  public static final String CONFIG_TYPE = "type";
  public static final String CONFIG_INPUTS = "inputs";
  public static final String CONFIG_INPUT_NAME = "name";
  public static final String CONFIG_INPUT_TYPE = "type";
  public static final String CONFIG_INPUT_SENSITIVE = "sensitive";
  public static final String CONFIG_INPUT_SIZE = "size";
  public static final String CONFIG_INPUT_VALUE = "value";
  public static final String CONFIG_INPUT_VALUES = "values";

  /**
   * Transform given list of configs to JSON Array object.
   *
   * @param mConfigs List of configs.
   * @return JSON object with serialized config of the list.
   */
  @SuppressWarnings("unchecked")
  public static JSONArray extractConfigList(List<MConfig> mConfigs, boolean skipSensitive) {
    JSONArray configs = new JSONArray();

    for (MConfig mConfig : mConfigs) {
      configs.add(extractConfig(mConfig, skipSensitive));
    }

    return configs;
  }

  /**
   * Transform given config to JSON Object.
   *
   * @param mConfig Given MConfig instance
   * @param skipSensitive conditionally add sensitive input values
   * @return Serialized JSON object.
   */
  @SuppressWarnings("unchecked")
  static JSONObject extractConfig(MConfig mConfig, boolean skipSensitive) {
    JSONObject config = new JSONObject();
    config.put(ID, mConfig.getPersistenceId());
    config.put(CONFIG_NAME, mConfig.getName());
    config.put(CONFIG_TYPE, MConfigType.LINK.toString());
    JSONArray mInputs = new JSONArray();
    config.put(CONFIG_INPUTS, mInputs);

    for (MInput<?> mInput : mConfig.getInputs()) {
      JSONObject input = new JSONObject();
      input.put(ID, mInput.getPersistenceId());
      input.put(CONFIG_INPUT_NAME, mInput.getName());
      input.put(CONFIG_INPUT_TYPE, mInput.getType().toString());
      input.put(CONFIG_INPUT_SENSITIVE, mInput.isSensitive());

      // String specific serialization
      if (mInput.getType() == MInputType.STRING) {
        input.put(CONFIG_INPUT_SIZE,
            ((MStringInput)mInput).getMaxLength());
      }

      // Enum specific serialization
      if(mInput.getType() == MInputType.ENUM) {
        input.put(CONFIG_INPUT_VALUES,
          StringUtils.join(((MEnumInput)mInput).getValues(), ","));
      }

      // Serialize value if is there
      // Skip if sensitive
      if (!mInput.isEmpty() && !(skipSensitive && mInput.isSensitive())) {
        if (mInput.getType() == MInputType.MAP) {
          input.put(CONFIG_INPUT_VALUE, mInput.getValue());
        } else {
          input.put(CONFIG_INPUT_VALUE, mInput.getUrlSafeValueString());
        }
      }

      mInputs.add(input);
    }

    return config;
  }

  /**
   * Restore List of MConfigs from JSON Array.
   *
   * @param configs JSON array representing list of MConfigs
   * @return Restored list of MConfigs
   */
  public static List<MConfig> restoreConfigList(JSONArray configs) {
    List<MConfig> mConfigs = new ArrayList<MConfig>();

    for (int i = 0; i < configs.size(); i++) {
      mConfigs.add(restoreConfig((JSONObject) configs.get(i)));
    }

    return mConfigs;
  }

  /**
   * Restore one MConfig from JSON Object.
   *
   * @param config JSON representation of the MConfig.
   * @return Restored MConfig.
   */
  static MConfig restoreConfig(JSONObject config) {
    JSONArray inputs = (JSONArray) config.get(CONFIG_INPUTS);

    List<MInput<?>> mInputs = new ArrayList<MInput<?>>();
    for (int i = 0; i < inputs.size(); i++) {
      JSONObject input = (JSONObject) inputs.get(i);
      MInputType type =
          MInputType.valueOf((String) input.get(CONFIG_INPUT_TYPE));
      String name = (String) input.get(CONFIG_INPUT_NAME);
      Boolean sensitive = (Boolean) input.get(CONFIG_INPUT_SENSITIVE);
      MInput mInput = null;
      switch (type) {
        case STRING: {
          long size = (Long) input.get(CONFIG_INPUT_SIZE);
          mInput = new MStringInput(name, sensitive.booleanValue(), (short) size);
          break;
        }
        case MAP: {
          mInput = new MMapInput(name, sensitive.booleanValue());
          break;
        }
        case INTEGER: {
          mInput = new MIntegerInput(name, sensitive.booleanValue());
          break;
        }
        case BOOLEAN: {
          mInput = new MBooleanInput(name, sensitive.booleanValue());
          break;
        }
        case ENUM: {
          String values = (String) input.get(CONFIG_INPUT_VALUES);
          mInput = new MEnumInput(name, sensitive.booleanValue(), values.split(","));
          break;
        }
      }

      // Propagate config ID
      mInput.setPersistenceId((Long)input.get(ID));

      // Propagate config optional value
      if(input.containsKey(CONFIG_INPUT_VALUE)) {
        switch (type) {
        case MAP:
          try {
            mInput.setValue((Map<String, String>)input.get(CONFIG_INPUT_VALUE));
          } catch (ClassCastException e) {
            throw new SqoopException(SerializationError.SERIALIZATION_001, name + " requires a 'map' value.");
          }
          break;
        default:
          mInput.restoreFromUrlSafeValueString(
              (String) input.get(CONFIG_INPUT_VALUE));
          break;
        }
      }
      mInputs.add(mInput);
    }

    MConfig mConfig = new MConfig((String) config.get(CONFIG_NAME), mInputs);
    mConfig.setPersistenceId((Long) config.get(ID));
    return mConfig;
  }

  private ConfigSerialization() {
    // Do not instantiate
  }
}
