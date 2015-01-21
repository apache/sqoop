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
package org.apache.sqoop.validation;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.ConfigurationClass;
import org.apache.sqoop.model.Config;
import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.utils.ClassUtils;
import org.apache.sqoop.validation.validators.AbstractValidator;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Validation runner that will run validators associated with given configuration
 * class or config object.
 *
 * Execution follows following rules:
 * * Run children first (Inputs -> Config -> Class)
 * * If any children is not suitable (canProceed = false), skip running parent
 *
 * Which means that config validator don't have to repeat it's input validators as it will
 * be never called if the input's are not valid. Similarly Class validators won't be called
 * unless all configs will pass validators.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ConfigValidationRunner {

  /**
   * Private cache of instantiated validators.
   *
   * We're expecting that this cache will be very small as the number of possible validators
   * is driven to high extent by the number of connectors and hence we don't have a cache
   * eviction at the moment.
   */
  private Map<Class<? extends AbstractValidator>, AbstractValidator> cache;

  public ConfigValidationRunner() {
    cache = new HashMap<Class<? extends AbstractValidator>, AbstractValidator>();
  }

  /**
   * Validate given configuration instance.
   *
   * @param config Configuration instance
   * @return
   */
  public ConfigValidationResult validate(Object config) {
    ConfigValidationResult result = new ConfigValidationResult();
    ConfigurationClass globalAnnotation = ConfigUtils.getConfigurationClassAnnotation(config, true);

    // Iterate over all declared config and call their validators
    for (Field field : config.getClass().getDeclaredFields()) {
      field.setAccessible(true);

      Config configAnnotation = ConfigUtils.getConfigAnnotation(field, false);
      if(configAnnotation == null) {
        continue;
      }

      String configName = ConfigUtils.getName(field, configAnnotation);
      ConfigValidationResult r = validateConfig(configName, ConfigUtils.getFieldValue(field, config));
      result.mergeValidatorResult(r);
    }

    // Call class validator only as long as we are in suitable state
    if(result.getStatus().canProceed())  {
      ConfigValidationResult r = validateArray("", config, globalAnnotation.validators());
      result.mergeValidatorResult(r);
    }

    return result;
  }

  /**
   * Validate given config instance.
   *
   * @param configName Config's name to build full name for all inputs.
   * @param config Config instance
   * @return
   */
  public ConfigValidationResult validateConfig(String configName, Object config) {
    ConfigValidationResult result = new ConfigValidationResult();
    ConfigClass configAnnotation = ConfigUtils.getConfigClassAnnotation(config, true);

    // Iterate over all declared inputs and call their validators
    for (Field field : config.getClass().getDeclaredFields()) {
      Input inputAnnotation = ConfigUtils.getInputAnnotation(field, false);
      if(inputAnnotation == null) {
        continue;
      }

      String name = configName + "." + ConfigUtils.getName(field, inputAnnotation);

      ConfigValidationResult r = validateArray(name, ConfigUtils.getFieldValue(field, config), inputAnnotation.validators());
      result.mergeValidatorResult(r);
    }

    // Call config validator only as long as we are in suitable state
    if(result.getStatus().canProceed())  {
      ConfigValidationResult r = validateArray(configName, config, configAnnotation.validators());
      result.mergeValidatorResult(r);
    }

    return result;
  }

  /**
   * Execute array of validators on given object (can be input/config/class).
   *
   * @param name Full name of the object
   * @param object Input, Config or Class instance
   * @param validators Validators array
   * @return
   */
  private ConfigValidationResult validateArray(String name, Object object, Validator[] validators) {
    ConfigValidationResult result = new ConfigValidationResult();

    for (Validator validator : validators) {
      AbstractValidator v = executeValidator(object, validator);
      result.addValidatorResult(name, v);
    }

    return result;
  }

  /**
   * Execute single validator.
   *
   * @param object Input, Config or Class instance
   * @param validator Validator annotation
   * @return
   */
  private AbstractValidator executeValidator(Object object, Validator validator) {
    // Try to get validator instance from the cache
    AbstractValidator instance = cache.get(validator.value());

    if(instance == null) {
      instance = (AbstractValidator) ClassUtils.instantiate(validator.value());

      // This could happen if we would be missing some connector's jars on our classpath
      if(instance == null) {
        throw new SqoopException(ConfigValidationError.VALIDATION_0004, validator.value().getName());
      }

      cache.put(validator.value(), instance);
    } else {
      instance.reset();
    }

    instance.setStringArgument(validator.strArg());
    instance.validate(object);
    return instance;
  }


}
