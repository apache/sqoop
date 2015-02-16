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
package org.apache.sqoop.model;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.CommonRepositoryError;
import org.apache.sqoop.json.JSONUtils;
import org.apache.sqoop.utils.ClassUtils;
import org.apache.sqoop.validation.ConfigValidationRunner;
import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.ConfigValidationResult;
import org.json.simple.JSONObject;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Util class for transforming data from correctly annotated configuration
 * objects to different structures and vice-versa.
 *
 * TODO: This class should see some overhaul into more reusable code, especially expose and re-use the methods at the end.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class  ConfigUtils {

  /**
   * Transform correctly annotated configuration object to corresponding
   * list of configs.
   *
   * Configs will be order according to the occurrence in the configuration
   * class. Inputs will be also ordered based on occurrence.
   *
   * @param configuration Annotated arbitrary configuration object
   * @return Corresponding list of configs
   */
  public static List<MConfig> toConfigs(Object configuration) {
    return toConfigs(configuration.getClass(), configuration);
  }

  public static List<MConfig> toConfigs(Class<?> klass) {
    return toConfigs(klass, null);
  }

  public static List<MConfig> toConfigs(Class<?> klass, Object configuration) {
    Set<String> configNames = new HashSet<String>();

    ConfigurationClass configurationClass =
      (ConfigurationClass)klass.getAnnotation(ConfigurationClass.class);

    // Each configuration object must have this class annotation
    if(configurationClass == null) {
      throw new SqoopException(ModelError.MODEL_003,
        "Missing annotation ConfigurationClass on class " + klass.getName());
    }

    List<MConfig> configs = new LinkedList<MConfig>();

    // Iterate over all declared fields
    for (Field field : klass.getDeclaredFields()) {
      field.setAccessible(true);

      // Each field that should be part of user input should have Input
      // annotation.
      Config configAnnotation = field.getAnnotation(Config.class);

      if (configAnnotation != null) {
        String configName = getConfigName(field, configAnnotation, configNames);

        Class<?> type = field.getType();

        Object value = null;
        if(configuration != null) {
          try {
            value = field.get(configuration);
          } catch (IllegalAccessException e) {
            throw new SqoopException(ModelError.MODEL_005,
              "Can't retrieve value from " + field.getName(), e);
          }
        }

        configs.add(toConfig(configName, type, value));
      }
    }

    return configs;
  }

  @SuppressWarnings("unchecked")
  private static MConfig toConfig(String configName, Class klass, Object object) {
     ConfigClass global =
      (ConfigClass)klass.getAnnotation(ConfigClass.class);

    // Each configuration object must have this class annotation
    if(global == null) {
      throw new SqoopException(ModelError.MODEL_003,
        "Missing annotation ConfigClass on class " + klass.getName());
    }

    // Intermediate list of inputs
    List<MInput<?>> inputs = new LinkedList<MInput<?>>();

    // Iterate over all declared fields
    for (Field field : klass.getDeclaredFields()) {
      field.setAccessible(true);

      String fieldName = field.getName();
      String inputName = configName + "." + fieldName;

      // Each field that should be part of user input should have Input
      // annotation.
      Input inputAnnotation = field.getAnnotation(Input.class);

      if(inputAnnotation != null) {
        boolean sensitive = inputAnnotation.sensitive();
        short maxLen = inputAnnotation.size();
        InputEditable editable = inputAnnotation.editable();
        String overrides = inputAnnotation.overrides();
        Class<?> type = field.getType();

        MInput input;

        // We need to support NULL, so we do not support primitive types
        if(type.isPrimitive()) {
          throw new SqoopException(ModelError.MODEL_007,
            "Detected primitive type " + type + " for field " + fieldName);
        }

        // Instantiate corresponding MInput<?> structure
        if (type == String.class) {
          input = new MStringInput(inputName, sensitive, editable, overrides, maxLen);
        } else if (type.isAssignableFrom(Map.class)) {
          input = new MMapInput(inputName, sensitive, editable, overrides);
        } else if (type == Integer.class) {
          input = new MIntegerInput(inputName, sensitive, editable, overrides);
        } else if (type == Long.class) {
          input = new MLongInput(inputName, sensitive, editable, overrides);
        } else if (type == Boolean.class) {
          input = new MBooleanInput(inputName, sensitive, editable, overrides);
        } else if (type.isEnum()) {
          input = new MEnumInput(inputName, sensitive, editable, overrides,
              ClassUtils.getEnumStrings(type));
        } else {
          throw new SqoopException(ModelError.MODEL_004, "Unsupported type "
              + type.getName() + " for input " + fieldName);
        }

        // Move value if it's present in original configuration object
        if(object != null) {
          Object value;
          try {
            value = field.get(object);
          } catch (IllegalAccessException e) {
            throw new SqoopException(ModelError.MODEL_005,
              "Can't retrieve value from " + field.getName(), e);
          }
          if(value == null) {
            input.setEmpty();
          } else {
            input.setValue(value);
          }
        }

        inputs.add(input);
      }
    }
    MConfig config = new MConfig(configName, inputs);
    // validation has to happen only when all inputs have been parsed
    for (MInput<?> input : config.getInputs()) {
      validateInputOverridesAttribute(input, config);
    }

    return config;
  }

  private static Field getFieldFromName(Class<?> klass, String name) {
    Field configField;
    try {
      configField = klass.getDeclaredField(name);
    } catch (NoSuchFieldException e) {
      // reverse lookup config field from custom config name
      if (name != null) {
        for (Field field : klass.getDeclaredFields()) {
          Config configAnnotation = field.getAnnotation(Config.class);
          if (configAnnotation == null) {
            continue;
          }
          if (!StringUtils.isEmpty(configAnnotation.name()) && name.equals(configAnnotation.name())) {
            return field;
          }
        }
      }
      throw new SqoopException(ModelError.MODEL_006, "Missing field " + name + " on config class "
          + klass.getCanonicalName(), e);
    }
    return configField;
  }

  /**
   * Convenience method to directly validate given model classes without the need to
   * manually create the configuration instance.
   *
   * @param configs Model representation with filled values
   * @param configClass Configuration class
   * @return Validation result
   */
  public static ConfigValidationResult validateConfigs(List<MConfig> configs, Class configClass) {
    ConfigValidationRunner validationRunner = new ConfigValidationRunner();
    Object configInstance = fromConfigs(configs, configClass);
    return validationRunner.validate(configInstance);
  }

  /**
   * Convenience method to convert given model structure into configuration object
   * that will be created from given class.
   *
   * @param configs Model representation with filled values
   * @param configClass Configuration class
   * @return Created instance based on the class with filled values
   */
  public static Object fromConfigs(List<MConfig> configs, Class configClass) {
    Object configInstance = ClassUtils.instantiate(configClass);
    if(configInstance == null) {
      throw new SqoopException(ModelError.MODEL_016, configClass.getCanonicalName());
    }

    fromConfigs(configs, configInstance);
    return configInstance;
  }

  /**
   * Move config values from config list into corresponding configuration object.
   *
   * @param configs Input config list
   * @param configuration Output configuration object
   */
  @SuppressWarnings("unchecked")
  public static void fromConfigs(List<MConfig> configs, Object configuration) {
    Class klass = configuration.getClass();

    for(MConfig config : configs) {
      Field configField;
      try {
        configField = klass.getDeclaredField(config.getName());
      } catch (NoSuchFieldException e) {
        throw new SqoopException(ModelError.MODEL_006,
          "Missing field " + config.getName() + " on config class " + klass.getCanonicalName(), e);
      }

      configField = getFieldFromName(klass, config.getName());
      // We need to access this field even if it would be declared as private
      configField.setAccessible(true);
      Class<?> configClass = configField.getType();
      Object newValue = ClassUtils.instantiate(configClass);

      if (newValue == null) {
        throw new SqoopException(ModelError.MODEL_006,
          "Can't instantiate new config " + configClass);
      }

      for(MInput input : config.getInputs()) {
        String[] splitNames = input.getName().split("\\.");
        if (splitNames.length != 2) {
          throw new SqoopException(ModelError.MODEL_009, "Invalid name: "
              + input.getName());
        }

        String inputName = splitNames[1];
        // TODO(jarcec): Names structures fix, handle error cases
        Field inputField;
        try {
          inputField = configClass.getDeclaredField(inputName);
        } catch (NoSuchFieldException e) {
          throw new SqoopException(ModelError.MODEL_006, "Missing field "
              + input.getName(), e);
        }

        // We need to access this field even if it would be declared as private
        inputField.setAccessible(true);

        try {
          if (input.isEmpty()) {
            inputField.set(newValue, null);
          } else {
            if (input.getType() == MInputType.ENUM) {
              inputField.set(newValue, Enum.valueOf(
                  (Class<? extends Enum>) inputField.getType(),
                  (String) input.getValue()));
            } else {
              inputField.set(newValue, input.getValue());
            }
          }
        } catch (IllegalAccessException e) {
          throw new SqoopException(ModelError.MODEL_005, "Issue with field "
              + inputField.getName(), e);
        }
      }

      try {
        configField.set(configuration, newValue);
      } catch (IllegalAccessException e) {
        throw new SqoopException(ModelError.MODEL_005,
          "Issue with field " + configField.getName(), e);
      }
    }
  }

  /**
   * Apply given validations on list of configs.
   *
   * @param configs
   * @param result
   */
  public static void applyValidation(List<MConfig> configs, ConfigValidationResult result) {
    for(MConfig config : configs) {
      applyValidation(config, result);

      for(MInput input : config.getInputs()) {
        applyValidation(input, result);
      }
    }
  }

  /**
   * Apply validation messages on given element.
   *
   * Element's state will be set to default if there are no associated messages.
   *
   * @param element
   * @param result
   */
  public static void applyValidation(MValidatedElement element, ConfigValidationResult result) {
    List<Message> messages = result.getMessages().get(element.getName());

    if(messages != null) {
      element.setValidationMessages(messages);
    } else {
      element.resetValidationMessages();
    }
  }

  /**
   * Convert configuration object to JSON. Only filled properties are serialized,
   * properties with null value are skipped.
   *
   * @param configuration Correctly annotated configuration object
   * @return String of JSON representation
   */
  @SuppressWarnings("unchecked")
  public static String toJson(Object configuration) {
    Class klass = configuration.getClass();
    Set<String> configNames = new HashSet<String>();
    ConfigurationClass configurationClass =
      (ConfigurationClass)klass.getAnnotation(ConfigurationClass.class);

    // Each configuration object must have this class annotation
    if(configurationClass == null) {
      throw new SqoopException(ModelError.MODEL_003,
        "Missing annotation ConfigurationGroup on class " + klass.getName());
    }

    JSONObject jsonOutput = new JSONObject();

    // Iterate over all declared fields
    for (Field configField : klass.getDeclaredFields()) {
      configField.setAccessible(true);

      // We're processing only config validations
      Config configAnnotation = configField.getAnnotation(Config.class);
      if(configAnnotation == null) {
        continue;
      }
      String configName = getConfigName(configField, configAnnotation, configNames);

      Object configValue;
      try {
        configValue = configField.get(configuration);
      } catch (IllegalAccessException e) {
        throw new SqoopException(ModelError.MODEL_005, "Issue with field "
            + configName, e);
      }

      JSONObject jsonConfig = new JSONObject();

      // Now process each input on the config
      for(Field inputField : configField.getType().getDeclaredFields()) {
        inputField.setAccessible(true);
        String inputName = inputField.getName();

        Object value;
        try {
          value = inputField.get(configValue);
        } catch (IllegalAccessException e) {
          throw new SqoopException(ModelError.MODEL_005, "Issue with field "
              + configName + "." + inputName, e);
        }

        Input inputAnnotation = inputField.getAnnotation(Input.class);

        // Do not serialize all values
        if (inputAnnotation != null && value != null) {
          Class<?> type = inputField.getType();

          // We need to support NULL, so we do not support primitive types
          if (type.isPrimitive()) {
            throw new SqoopException(ModelError.MODEL_007,
                "Detected primitive type " + type + " for field " + configName
                    + "." + inputName);
          }

          if(type == String.class) {
            jsonConfig.put(inputName, value);
          } else if (type.isAssignableFrom(Map.class)) {
            JSONObject map = new JSONObject();
            for (Object key : ((Map) value).keySet()) {
              map.put(key, ((Map) value).get(key));
            }
            jsonConfig.put(inputName, map);
          } else if(type == Integer.class || type == Long.class) {
            jsonConfig.put(inputName, value);
          } else if(type.isEnum()) {
            jsonConfig.put(inputName, value.toString());
          } else if(type == Boolean.class) {
            jsonConfig.put(inputName, value);
          }else {
            throw new SqoopException(ModelError.MODEL_004,
              "Unsupported type " + type.getName() + " for input " + configName + "." + inputName);
          }
        }
      }

      jsonOutput.put(configName, jsonConfig);
    }
    return jsonOutput.toJSONString();
  }

  /**
   * Parse given input JSON string and move it's values to given configuration
   * object.
   *
   * @param json JSON representation of the configuration object
   * @param configuration ConfigurationGroup object to be filled
   */
  @SuppressWarnings("unchecked")
  public static void fillValues(String json, Object configuration) {
    Class<?> klass = configuration.getClass();

    Set<String> configNames = new HashSet<String>();
    JSONObject jsonConfigs = JSONUtils.parse(json);

    for(Field configField : klass.getDeclaredFields()) {
      configField.setAccessible(true);
      String configName = configField.getName();

      // We're processing only config validations
      Config configAnnotation = configField.getAnnotation(Config.class);
      if(configAnnotation == null) {
        continue;
      }
      configName = getConfigName(configField, configAnnotation, configNames);

      try {
        configField.set(configuration, configField.getType().newInstance());
      } catch (Exception e) {
        throw new SqoopException(ModelError.MODEL_005,
          "Issue with field " + configName, e);
      }

      JSONObject jsonInputs = (JSONObject) jsonConfigs.get(configField.getName());
      if(jsonInputs == null) {
        continue;
      }

      Object configValue;
      try {
        configValue = configField.get(configuration);
      } catch (IllegalAccessException e) {
        throw new SqoopException(ModelError.MODEL_005,
          "Issue with field " + configName, e);
      }

      for(Field inputField : configField.getType().getDeclaredFields()) {
        inputField.setAccessible(true);
        String inputName = inputField.getName();

        Input inputAnnotation = inputField.getAnnotation(Input.class);

        if(inputAnnotation == null || jsonInputs.get(inputName) == null) {
          try {
            inputField.set(configValue, null);
          } catch (IllegalAccessException e) {
            throw new SqoopException(ModelError.MODEL_005,
              "Issue with field " + configName + "." + inputName, e);
          }
          continue;
        }

        Class<?> type = inputField.getType();

        try {
          if(type == String.class) {
            inputField.set(configValue, jsonInputs.get(inputName));
          } else if (type.isAssignableFrom(Map.class)) {
            Map<String, String> map = new HashMap<String, String>();
            JSONObject jsonObject = (JSONObject) jsonInputs.get(inputName);
            for(Object key : jsonObject.keySet()) {
              map.put((String)key, (String)jsonObject.get(key));
            }
            inputField.set(configValue, map);
          } else if(type == Integer.class) {
            inputField.set(configValue, ((Long)jsonInputs.get(inputName)).intValue());
          } else if(type == Long.class) {
            inputField.set(configValue, ((Long)jsonInputs.get(inputName)).longValue());
          } else if(type.isEnum()) {
            inputField.set(configValue, Enum.valueOf((Class<? extends Enum>) inputField.getType(), (String) jsonInputs.get(inputName)));
          } else if(type == Boolean.class) {
            inputField.set(configValue, (Boolean) jsonInputs.get(inputName));
          }else {
            throw new SqoopException(ModelError.MODEL_004,
              "Unsupported type " + type.getName() + " for input " + configName + "." + inputName);
          }
        } catch (IllegalAccessException e) {
          throw new SqoopException(ModelError.MODEL_005,
            "Issue with field " + configName + "." + inputName, e);
        }
      }
    }
  }

  private static String getConfigName(Field member, Config annotation, Set<String> existingConfigNames) {
    if (StringUtils.isEmpty(annotation.name())) {
      return member.getName();
    } else {
      checkForValidConfigName(existingConfigNames, annotation.name());
      existingConfigNames.add(annotation.name());
      return annotation.name();
    }
  }

  private static void checkForValidConfigName(Set<String> existingConfigNames,
      String customConfigName) {
    // uniqueness across fields check
    if (existingConfigNames.contains(customConfigName)) {
      throw new SqoopException(ModelError.MODEL_012,
          "Issue with field config name " + customConfigName);
    }

    if (!Character.isJavaIdentifierStart(customConfigName.toCharArray()[0])) {
      throw new SqoopException(ModelError.MODEL_013,
          "Issue with field config name " + customConfigName);
    }
    for (Character c : customConfigName.toCharArray()) {
      if (Character.isJavaIdentifierPart(c))
        continue;
      throw new SqoopException(ModelError.MODEL_013,
          "Issue with field config name " + customConfigName);
    }

    if (customConfigName.length() > 30) {
      throw new SqoopException(ModelError.MODEL_014,
          "Issue with field config name " + customConfigName);

    }
  }

  public static String getName(Field input, Input annotation) {
    return input.getName();
  }

  public static String getName(Field config, Config annotation) {
    return config.getName();
  }

  public static ConfigurationClass getConfigurationClassAnnotation(Object object, boolean strict) {
    ConfigurationClass annotation = object.getClass().getAnnotation(ConfigurationClass.class);

    if(strict && annotation == null) {
      throw new SqoopException(ModelError.MODEL_003, "Missing annotation ConfigurationGroupClass on class " + object.getClass().getName());
    }

    return annotation;
  }

  public static ConfigClass getConfigClassAnnotation(Object object, boolean strict) {
    ConfigClass annotation = object.getClass().getAnnotation(ConfigClass.class);

    if(strict && annotation == null) {
      throw new SqoopException(ModelError.MODEL_003, "Missing annotation ConfigurationGroupClass on class " + object.getClass().getName());
    }

    return annotation;
  }

  public static Config getConfigAnnotation(Field field, boolean strict) {
    Config annotation = field.getAnnotation(Config.class);

    if(strict && annotation == null) {
      throw new SqoopException(ModelError.MODEL_003, "Missing annotation Config on Field " + field.getName() + " on class " + field.getDeclaringClass().getName());
    }

    return annotation;
  }

  public static Input getInputAnnotation(Field field, boolean strict) {
    Input annotation = field.getAnnotation(Input.class);

    if(strict && annotation == null) {
      throw new SqoopException(ModelError.MODEL_003, "Missing annotation Input on Field " + field.getName() + " on class " + field.getDeclaringClass().getName());
    }

    return annotation;
  }

  public static Object getFieldValue(Field field, Object object) {
    try {
      field.setAccessible(true);
      return field.get(object);
    } catch (IllegalAccessException e) {
      throw new SqoopException(ModelError.MODEL_015, e);
    }
  }

  /**
   * Validate that the input override attribute adheres to the rules imposed
   * NOTE: all input names in a config class will and must be unique, check the name exists and it is not self override
   * Rule #1.
   * If editable == USER_ONLY ( cannot override itself ) can override other  CONNECTOR_ONLY and ANY inputs,
   * but cannot overriding other USER_ONLY attributes
   * Rule #2.
   * If editable == CONNECTOR_ONLY or ANY ( cannot override itself ) can override any other attribute in the config object
   * @param currentInput
   *
   */
  public static void validateInputOverridesAttribute(MInput<?> currentInput, MConfig config) {

    // split the overrides string into comma separated list
    String overrides = currentInput.getOverrides();
    if (StringUtils.isEmpty(overrides)) {
      return;
    }
    String[] overrideInputs = overrides.split("\\,");
    for (String override : overrideInputs) {
      if (!config.getInputNames().contains(override)) {
        throw new SqoopException(ModelError.MODEL_017, "for input :"
            + currentInput.toString());
      }
      if (override.equals(currentInput.getName())) {
        throw new SqoopException(ModelError.MODEL_018, "for input :"
            + currentInput.toString());
      }
      if (currentInput.getEditable().equals(InputEditable.USER_ONLY)) {
        if (config.getUserOnlyEditableInputNames().contains(override)) {
          throw new SqoopException(ModelError.MODEL_019, "for input :"
              + currentInput.toString());
        }
      }
    }
    return;
  }
}