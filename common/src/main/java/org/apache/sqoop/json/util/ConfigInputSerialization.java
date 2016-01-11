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
import org.apache.sqoop.json.JSONUtils;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MDateTimeInput;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigType;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MListInput;
import org.apache.sqoop.model.MLongInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MValidator;
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
   * @param mConfigList List of configs.
   * @return JSON object with serialized config of the list.
   */
  @SuppressWarnings("unchecked")
  public static JSONObject extractConfigList(MConfigList mConfigList, boolean skipSensitive) {
    JSONObject jsonConfigList = new JSONObject();

    jsonConfigList.put(ConfigInputConstants.CONFIG_VALIDATORS, extractValidators(mConfigList.getValidators()));

    JSONArray configs = new JSONArray();

    for (MConfig mConfig : mConfigList.getConfigs()) {
      configs.add(extractConfig(mConfig, mConfigList.getType(), skipSensitive));
    }

    jsonConfigList.put(ConfigInputConstants.CONFIGS, configs);
    return jsonConfigList;
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

    config.put(ConfigInputConstants.CONFIG_VALIDATORS, extractValidators(mConfig.getValidators()));

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

      input.put(ConfigInputConstants.CONFIG_VALIDATORS, extractValidators(mInput.getValidators()));

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
          MMapInput mMapInput = (MMapInput)mInput;
          input.put(ConfigInputConstants.CONFIG_INPUT_SENSITIVE_KEY_PATTERN, mMapInput.getSensitiveKeyPattern());
          if (skipSensitive) {
            input.put(ConfigInputConstants.CONFIG_INPUT_VALUE, mMapInput.getNonsenstiveValue());
          } else {
            input.put(ConfigInputConstants.CONFIG_INPUT_VALUE, mMapInput.getValue());
          }
        } else {
          input.put(ConfigInputConstants.CONFIG_INPUT_VALUE, mInput.getUrlSafeValueString());
        }
      }

      mInputs.add(input);
    }

    return config;
  }

  /**
   * Extract list of MValidators to JSONArray.
   *
   * @param mValidators List of MValidators
   * @return JSONArray containing json objects representing the MValidators
   */
  public static JSONArray extractValidators(List<MValidator> mValidators) {
    JSONArray jsonValidators = new JSONArray();
    for (MValidator mValidator : mValidators) {
      JSONObject jsonValidator = new JSONObject();
      jsonValidator.put(ConfigValidatorConstants.VALIDATOR_CLASS, mValidator.getValidatorClass());
      jsonValidator.put(ConfigValidatorConstants.VALIDATOR_STR_ARG, mValidator.getStrArg());
      jsonValidators.add(jsonValidator);
    }
    return jsonValidators;
  }

  /**
   * Restore List of MValidations from JSON Array.
   *
   * @param jsonValidators JSON array representing list of MValidators
   * @return Restored list of MValidations
   */
  public static List<MValidator> restoreValidator(JSONArray jsonValidators) {
    List<MValidator> mValidators = new ArrayList<>();
    for (int validatorCounter = 0; validatorCounter < jsonValidators.size(); validatorCounter++) {
      JSONObject jsonValidator = (JSONObject) jsonValidators.get(validatorCounter);
      String validatorClassName = JSONUtils.getString(jsonValidator, ConfigValidatorConstants.VALIDATOR_CLASS);
      String validatorStrArg = JSONUtils.getString(jsonValidator, ConfigValidatorConstants.VALIDATOR_STR_ARG);
      mValidators.add(new MValidator(validatorClassName, validatorStrArg));
    }
    return mValidators;
  }

  /**
   * Restore List of MConfigs from JSON Array.
   *
   * @param configs JSON array representing list of MConfigs
   * @return Restored list of MConfigs
   */
  public static List<MConfig> restoreConfigs(JSONArray configs) {
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
    JSONArray inputs = JSONUtils.getJSONArray(config, ConfigInputConstants.CONFIG_INPUTS);

    List<MInput<?>> mInputs = new ArrayList<MInput<?>>();
    for (int i = 0; i < inputs.size(); i++) {
      JSONObject input = (JSONObject) inputs.get(i);
      MInputType type =  MInputType.valueOf(JSONUtils.getString(input, ConfigInputConstants.CONFIG_INPUT_TYPE));
      String name = JSONUtils.getString(input, ConfigInputConstants.CONFIG_INPUT_NAME);
      Boolean sensitive = JSONUtils.getBoolean(input, ConfigInputConstants.CONFIG_INPUT_SENSITIVE);
      InputEditable editable =  (input.containsKey(ConfigInputConstants.CONFIG_INPUT_EDITABLE)) ? InputEditable.valueOf(JSONUtils.getString(input, ConfigInputConstants.CONFIG_INPUT_EDITABLE)) : InputEditable.USER_ONLY;
      String overrides = JSONUtils.getString(input, ConfigInputConstants.CONFIG_INPUT_OVERRIDES);
      String sensitveKeyPattern = input.containsKey(ConfigInputConstants.CONFIG_INPUT_SENSITIVE_KEY_PATTERN) ? JSONUtils.getString(input, ConfigInputConstants.CONFIG_INPUT_SENSITIVE_KEY_PATTERN) : null;
      List<MValidator> mValidatorsForInput = restoreValidator(JSONUtils.getJSONArray(input, ConfigInputConstants.CONFIG_VALIDATORS));

      MInput mInput = null;
      switch (type) {
      case STRING: {
        long size = JSONUtils.getLong(input, ConfigInputConstants.CONFIG_INPUT_SIZE);
        mInput = new MStringInput(name, sensitive.booleanValue(), editable, overrides, (short) size, mValidatorsForInput);
        break;
      }
      case MAP: {
        mInput = new MMapInput(name, sensitive.booleanValue(), editable, overrides, sensitveKeyPattern, mValidatorsForInput);
        break;
      }
      case INTEGER: {
        mInput = new MIntegerInput(name, sensitive.booleanValue(), editable, overrides, mValidatorsForInput);
        break;
      }
      case LONG: {
        mInput = new MLongInput(name, sensitive.booleanValue(), editable, overrides, mValidatorsForInput);
        break;
      }
      case BOOLEAN: {
        mInput = new MBooleanInput(name, sensitive.booleanValue(), editable, overrides, mValidatorsForInput);
        break;
      }
      case ENUM: {
        String values = JSONUtils.getString(input, ConfigInputConstants.CONFIG_INPUT_ENUM_VALUES);
        mInput = new MEnumInput(name, sensitive.booleanValue(), editable, overrides, values.split(","), mValidatorsForInput);
        break;
      }
      case LIST: {
        mInput = new MListInput(name, sensitive.booleanValue(), editable, overrides, mValidatorsForInput);
        break;
      }
      case DATETIME: {
        mInput = new MDateTimeInput(name, sensitive.booleanValue(), editable, overrides, mValidatorsForInput);
        break;
      }
      default:
        // do nothing
        break;
      }

      // Propagate config ID
      Long id = JSONUtils.getLong(input, ConfigInputConstants.INPUT_ID);
      if(id == null) {
        throw new SqoopException(SerializationError.SERIALIZATION_002, "Missing field: " + ConfigInputConstants.INPUT_ID);
      }
      mInput.setPersistenceId(id);

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
          mInput.restoreFromUrlSafeValueString(JSONUtils.getString(input, ConfigInputConstants.CONFIG_INPUT_VALUE));
          break;
        }
      }
      mInputs.add(mInput);
    }

    List<MValidator> mValidatorsForConfig = restoreValidator(JSONUtils.getJSONArray(config, ConfigInputConstants.CONFIG_VALIDATORS));
    MConfig mConfig = new MConfig(JSONUtils.getString(config, ConfigInputConstants.CONFIG_NAME), mInputs, mValidatorsForConfig);
    mConfig.setPersistenceId(JSONUtils.getLong(config, ConfigInputConstants.CONFIG_ID));
    return mConfig;
  }

  private ConfigInputSerialization() {
    // Do not instantiate
  }
}
