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
import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigType;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MLongInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Convenient static methods for serializing config and input objects.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ConfigInputSerialization {

  /**
   * Transform given list of configs to JSON Array object.
   *
   * @param mConfigs List of configs.
   * @return JSON object with serialized config of the list.
   */
  @SuppressWarnings("unchecked")
  public static JSONArray extractConfigList(List<MConfig> mConfigs, MConfigType type,
      boolean skipSensitive) {
    JSONArray configs = new JSONArray();

    for (MConfig mConfig : mConfigs) {
      configs.add(extractConfig(mConfig, type, skipSensitive));
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
  static JSONObject extractConfig(MConfig mConfig, MConfigType type, boolean skipSensitive) {
    JSONObject config = new JSONObject();
    config.put(ConfigInputConstants.CONFIG_ID, mConfig.getPersistenceId());
    config.put(ConfigInputConstants.CONFIG_NAME, mConfig.getName());
    config.put(ConfigInputConstants.CONFIG_TYPE, type.name());
    JSONArray mInputs = new JSONArray();
    config.put(ConfigInputConstants.CONFIG_INPUTS, mInputs);

    for (MInput<?> mInput : mConfig.getInputs()) {
      JSONObject input = new JSONObject();
      input.put(ConfigInputConstants.CONFIG_ID, mInput.getPersistenceId());
      input.put(ConfigInputConstants.CONFIG_INPUT_NAME, mInput.getName());
      input.put(ConfigInputConstants.CONFIG_INPUT_TYPE, mInput.getType().toString());
      input.put(ConfigInputConstants.CONFIG_INPUT_SENSITIVE, mInput.isSensitive());
      input.put(ConfigInputConstants.CONFIG_INPUT_EDITABLE, mInput.getEditable().name());
      input.put(ConfigInputConstants.CONFIG_INPUT_OVERRIDES, mInput.getOverrides());

      // String specific serialization
      if (mInput.getType() == MInputType.STRING) {
        input.put(ConfigInputConstants.CONFIG_INPUT_SIZE,
            ((MStringInput)mInput).getMaxLength());
      }

      // Enum specific serialization
      if(mInput.getType() == MInputType.ENUM) {
        input.put(ConfigInputConstants.CONFIG_INPUT_ENUM_VALUES,
          StringUtils.join(((MEnumInput)mInput).getValues(), ","));
      }

      // Serialize value if is there
      // Skip if sensitive
      if (!mInput.isEmpty() && !(skipSensitive && mInput.isSensitive())) {
        if (mInput.getType() == MInputType.MAP) {
          input.put(ConfigInputConstants.CONFIG_INPUT_VALUE, mInput.getValue());
        } else {
          input.put(ConfigInputConstants.CONFIG_INPUT_VALUE, mInput.getUrlSafeValueString());
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
  @SuppressWarnings({ "rawtypes", "unchecked" })
  static MConfig restoreConfig(JSONObject config) {
    JSONArray inputs = (JSONArray) config.get(ConfigInputConstants.CONFIG_INPUTS);

    List<MInput<?>> mInputs = new ArrayList<MInput<?>>();
    for (int i = 0; i < inputs.size(); i++) {
      JSONObject input = (JSONObject) inputs.get(i);
      MInputType type =
          MInputType.valueOf((String) input.get(ConfigInputConstants.CONFIG_INPUT_TYPE));
      String name = (String) input.get(ConfigInputConstants.CONFIG_INPUT_NAME);
      Boolean sensitive = (Boolean) input.get(ConfigInputConstants.CONFIG_INPUT_SENSITIVE);
      InputEditable editable =  (input.containsKey(ConfigInputConstants.CONFIG_INPUT_EDITABLE)) ?
          InputEditable.valueOf((String)input.get(ConfigInputConstants.CONFIG_INPUT_EDITABLE))
              : InputEditable.USER_ONLY;
      String overrides = (String) input.get(ConfigInputConstants.CONFIG_INPUT_OVERRIDES);

      MInput mInput = null;
      switch (type) {
      case STRING: {
        long size = (Long) input.get(ConfigInputConstants.CONFIG_INPUT_SIZE);
        mInput = new MStringInput(name, sensitive.booleanValue(), editable, overrides, (short) size);
        break;
      }
      case MAP: {
        mInput = new MMapInput(name, sensitive.booleanValue(), editable, overrides);
        break;
      }
      case INTEGER: {
        mInput = new MIntegerInput(name, sensitive.booleanValue(), editable, overrides);
        break;
      }
      case LONG: {
        mInput = new MLongInput(name, sensitive.booleanValue(), editable, overrides);
        break;
      }
      case BOOLEAN: {
        mInput = new MBooleanInput(name, sensitive.booleanValue(), editable, overrides);
        break;
      }
      case ENUM: {
        String values = (String) input.get(ConfigInputConstants.CONFIG_INPUT_ENUM_VALUES);
        mInput = new MEnumInput(name, sensitive.booleanValue(), editable, overrides, values.split(","));
        break;
      }
      default:
        // do nothing
        break;
      }

      // Propagate config ID
      mInput.setPersistenceId((Long)input.get(ConfigInputConstants.INPUT_ID));

      // Propagate config optional value
      if(input.containsKey(ConfigInputConstants.CONFIG_INPUT_VALUE)) {
        switch (type) {
        case MAP:
          try {
            mInput.setValue((Map<String, String>)input.get(ConfigInputConstants.CONFIG_INPUT_VALUE));
          } catch (ClassCastException e) {
            throw new SqoopException(SerializationError.SERIALIZATION_001, name + " requires a 'map' value.");
          }
          break;
        default:
          mInput.restoreFromUrlSafeValueString(
              (String) input.get(ConfigInputConstants.CONFIG_INPUT_VALUE));
          break;
        }
      }
      mInputs.add(mInput);
    }

    MConfig mConfig = new MConfig((String) config.get(ConfigInputConstants.CONFIG_NAME), mInputs);
    mConfig.setPersistenceId((Long) config.get(ConfigInputConstants.CONFIG_ID));
    return mConfig;
  }

  private ConfigInputSerialization() {
    // Do not instantiate
  }
}
