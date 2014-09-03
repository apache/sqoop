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

import org.apache.sqoop.model.ConfigurationClass;
import org.apache.sqoop.model.Form;
import org.apache.sqoop.model.FormClass;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.utils.ClassUtils;
import org.apache.sqoop.validation.validators.AbstractValidator;

import java.lang.reflect.Field;

/**
 * Validation runner that will run validators associated with given configuration
 * class or form object.
 *
 * Execution follows following rules:
 * * Run children first (Inputs -> Form -> Class)
 * * If any children is not suitable (canProceed = false), skip running parent
 *
 * Which means that form validator don't have to repeat it's input validators as it will
 * be never called if the input's are not valid. Similarly Class validators won't be called
 * unless all forms will pass validators.
 *
 * TODO: Cache the validators instances, so that we don't have create new instance every time
 */
public class ValidationRunner {

  /**
   * Validate given configuration instance.
   *
   * @param config Configuration instance
   * @return
   */
  public ValidationResult validate(Object config) {
    ValidationResult result = new ValidationResult();
    ConfigurationClass globalAnnotation = FormUtils.getConfigurationClassAnnotation(config, true);

    // Iterate over all declared form and call their validators
    for (Field field : config.getClass().getDeclaredFields()) {
      field.setAccessible(true);

      Form formAnnotation = FormUtils.getFormAnnotation(field, false);
      if(formAnnotation == null) {
        continue;
      }

      String formName = FormUtils.getName(field, formAnnotation);
      ValidationResult r = validateForm(formName, FormUtils.getFieldValue(field, config));
      result.merge(r);
    }

    // Call class validator only as long as we are in suitable state
    if(result.getStatus().canProceed())  {
      ValidationResult r = validateArray("", config, globalAnnotation.validators());
      result.merge(r);
    }

    return result;
  }

  /**
   * Validate given form instance.
   *
   * @param formName Form's name to build full name for all inputs.
   * @param form Form instance
   * @return
   */
  public ValidationResult validateForm(String formName, Object form) {
    ValidationResult result = new ValidationResult();
    FormClass formAnnotation = FormUtils.getFormClassAnnotation(form, true);

    // Iterate over all declared inputs and call their validators
    for (Field field : form.getClass().getDeclaredFields()) {
      Input inputAnnotation = FormUtils.getInputAnnotation(field, false);
      if(inputAnnotation == null) {
        continue;
      }

      String name = formName + "." + FormUtils.getName(field, inputAnnotation);

      ValidationResult r = validateArray(name, FormUtils.getFieldValue(field, form), inputAnnotation.validators());
      result.merge(r);
    }

    // Call form validator only as long as we are in suitable state
    if(result.getStatus().canProceed())  {
      ValidationResult r = validateArray(formName, form, formAnnotation.validators());
      result.merge(r);
    }

    return result;
  }

  /**
   * Execute array of validators on given object (can be input/form/class).
   *
   * @param name Full name of the object
   * @param object Input, Form or Class instance
   * @param validators Validators array
   * @return
   */
  private ValidationResult validateArray(String name, Object object, Validator[] validators) {
    ValidationResult result = new ValidationResult();

    for (Validator validator : validators) {
      AbstractValidator v = executeValidator(object, validator);
      result.addValidator(name, v);
    }

    return result;
  }

  /**
   * Execute single validator.
   *
   * @param object Input, Form or Class instance
   * @param validator Validator annotation
   * @return
   */
  private AbstractValidator executeValidator(Object object, Validator validator) {
    AbstractValidator instance = (AbstractValidator) ClassUtils.instantiate(validator.value());
    instance.setStringArgument(validator.strArg());
    instance.validate(object);
    return instance;
  }


}
