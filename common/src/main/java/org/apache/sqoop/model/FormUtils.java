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

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.utils.ClassUtils;
import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.apache.sqoop.validation.ValidationResult;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Util class for transforming data from correctly annotated configuration
 * objects to different structures and vice-versa.
 *
 * TODO: This class should see some overhaul into more reusable code, especially expose and re-use the methods at the end.
 */
public class FormUtils {

  /**
   * Transform correctly annotated configuration object to corresponding
   * list of forms.
   *
   * Forms will be order according to the occurrence in the configuration
   * class. Inputs will be also ordered based on occurrence.
   *
   * @param configuration Annotated arbitrary configuration object
   * @return Corresponding list of forms
   */
  public static List<MForm> toForms(Object configuration) {
    return toForms(configuration.getClass(), configuration);
  }

  public static List<MForm> toForms(Class klass) {
    return toForms(klass, null);
  }

  @SuppressWarnings("unchecked")
  public static List<MForm> toForms(Class klass, Object configuration) {
    ConfigurationClass global =
      (ConfigurationClass)klass.getAnnotation(ConfigurationClass.class);

    // Each configuration object must have this class annotation
    if(global == null) {
      throw new SqoopException(ModelError.MODEL_003,
        "Missing annotation ConfigurationClass on class " + klass.getName());
    }

    List<MForm> forms = new LinkedList<MForm>();

    // Iterate over all declared fields
    for (Field field : klass.getDeclaredFields()) {
      field.setAccessible(true);

      String formName = field.getName();

      // Each field that should be part of user input should have Input
      // annotation.
      Form formAnnotation = field.getAnnotation(Form.class);

      if(formAnnotation != null) {
        Class type = field.getType();

        Object value = null;
        if(configuration != null) {
          try {
            value = field.get(configuration);
          } catch (IllegalAccessException e) {
            throw new SqoopException(ModelError.MODEL_005,
              "Can't retrieve value from " + field.getName(), e);
          }
        }

        forms.add(toForm(formName, type, value));
      }
    }

    return forms;
  }

  @SuppressWarnings("unchecked")
  private static MForm toForm(String formName, Class klass, Object object) {
     FormClass global =
      (FormClass)klass.getAnnotation(FormClass.class);

    // Each configuration object must have this class annotation
    if(global == null) {
      throw new SqoopException(ModelError.MODEL_003,
        "Missing annotation FormClass on class " + klass.getName());
    }

    // Intermediate list of inputs
    List<MInput<?>> inputs = new LinkedList<MInput<?>>();

    // Iterate over all declared fields
    for (Field field : klass.getDeclaredFields()) {
      field.setAccessible(true);

      String fieldName = field.getName();
      String inputName = formName + "." + fieldName;

      // Each field that should be part of user input should have Input
      // annotation.
      Input inputAnnotation = field.getAnnotation(Input.class);

      if(inputAnnotation != null) {
        boolean sensitive = inputAnnotation.sensitive();
        short maxLen = inputAnnotation.size();
        Class type = field.getType();

        MInput input;

        // We need to support NULL, so we do not support primitive types
        if(type.isPrimitive()) {
          throw new SqoopException(ModelError.MODEL_007,
            "Detected primitive type " + type + " for field " + fieldName);
        }

        // Instantiate corresponding MInput<?> structure
        if(type == String.class) {
          input = new MStringInput(inputName, sensitive, maxLen);
        } else if (type.isAssignableFrom(Map.class)) {
          input = new MMapInput(inputName, sensitive);
        } else if(type == Integer.class) {
          input = new MIntegerInput(inputName, sensitive);
        } else if(type == Boolean.class) {
          input = new MBooleanInput(inputName, sensitive);
        } else if(type.isEnum()) {
          input = new MEnumInput(inputName, sensitive, ClassUtils.getEnumStrings(type));
        } else {
          throw new SqoopException(ModelError.MODEL_004,
            "Unsupported type " + type.getName() + " for input " + fieldName);
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

    return new MForm(formName, inputs);
  }

  /**
   * Move form values from form list into corresponding configuration object.
   *
   * @param forms Input form list
   * @param configuration Output configuration object
   */
  public static void fromForms(List<MForm> forms, Object configuration) {
    Class klass = configuration.getClass();

    for(MForm form : forms) {
      Field formField;
      try {
        formField = klass.getDeclaredField(form.getName());
      } catch (NoSuchFieldException e) {
        throw new SqoopException(ModelError.MODEL_006,
          "Missing field " + form.getName() + " on form class " + klass.getCanonicalName(), e);
      }

      // We need to access this field even if it would be declared as private
      formField.setAccessible(true);

      Class formClass = formField.getType();
      Object newValue = ClassUtils.instantiate(formClass);

      if(newValue == null) {
        throw new SqoopException(ModelError.MODEL_006,
          "Can't instantiate new form " + formClass);
      }

      for(MInput input : form.getInputs()) {
        String[] splitNames = input.getName().split("\\.");
        if(splitNames.length != 2) {
          throw new SqoopException(ModelError.MODEL_009,
            "Invalid name: " + input.getName());
        }

        String inputName = splitNames[1];
        // TODO(jarcec): Names structures fix, handle error cases
        Field inputField;
        try {
          inputField = formClass.getDeclaredField(inputName);
        } catch (NoSuchFieldException e) {
          throw new SqoopException(ModelError.MODEL_006,
            "Missing field " + input.getName(), e);
        }

        // We need to access this field even if it would be declared as private
        inputField.setAccessible(true);

        try {
          if(input.isEmpty()) {
            inputField.set(newValue, null);
          } else {
            if (input.getType() == MInputType.ENUM) {
              inputField.set(newValue, Enum.valueOf((Class<? extends Enum>)inputField.getType(), (String) input.getValue()));
            } else {
              inputField.set(newValue, input.getValue());
            }
          }
        } catch (IllegalAccessException e) {
          throw new SqoopException(ModelError.MODEL_005,
            "Issue with field " + inputField.getName(), e);
        }
      }

      try {
        formField.set(configuration, newValue);
      } catch (IllegalAccessException e) {
        throw new SqoopException(ModelError.MODEL_005,
          "Issue with field " + formField.getName(), e);
      }
    }
  }

  /**
   * Apply validations on the forms.
   *
   * @param forms Forms that should be updated
   * @param validation Validation that we should apply
   */
  public static void applyValidation(List<MForm> forms, Validation validation) {
    Map<Validation.FormInput, Validation.Message> messages = validation.getMessages();

    for(MForm form : forms) {
      applyValidation(form, messages);

      for(MInput input : form.getInputs()) {
        applyValidation(input, messages);
      }
    }
  }

  /**
   * Apply validation on given validated element.
   *
   * @param element Element on what we're applying the validations
   * @param messages Map of all validation messages
   */
  public static void applyValidation(MValidatedElement element, Map<Validation.FormInput, Validation.Message> messages) {
    Validation.FormInput name = new Validation.FormInput(element.getName());

    if(messages.containsKey(name)) {
      Validation.Message message = messages.get(name);
      element.setValidationMessage(message.getStatus(), message.getMessage());
    } else {
      element.setValidationMessage(Status.getDefault(), null);
    }
  }


  /**
   * Apply given validations on list of forms.
   *
   * @param forms
   * @param result
   */
  public static void applyValidation(List<MForm> forms, ValidationResult result) {
    for(MForm form : forms) {
      applyValidation(form, result);

      for(MInput input : form.getInputs()) {
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
  public static void applyValidation(MValidatedElement element, ValidationResult result) {
    List<Message> messages = result.getMessages().get(element.getName());

    if(messages != null) {
      // TODO(SQOOP-1465) Add support for multiple messages (showing only the first one for now)
      Message message = messages.get(0);
      element.setValidationMessage(message.getStatus(), message.getMessage());
    } else {
      element.setValidationMessage(Status.getDefault(), null);
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

    ConfigurationClass global =
      (ConfigurationClass)klass.getAnnotation(ConfigurationClass.class);

    // Each configuration object must have this class annotation
    if(global == null) {
      throw new SqoopException(ModelError.MODEL_003,
        "Missing annotation Configuration on class " + klass.getName());
    }

    JSONObject jsonOutput = new JSONObject();

    // Iterate over all declared fields
    for (Field formField : klass.getDeclaredFields()) {
      formField.setAccessible(true);
      String formName = formField.getName();

      // We're processing only form validations
      Form formAnnotation = formField.getAnnotation(Form.class);
      if(formAnnotation == null) {
        continue;
      }

      Object formValue;
      try {
        formValue = formField.get(configuration);
      } catch (IllegalAccessException e) {
        throw new SqoopException(ModelError.MODEL_005,
          "Issue with field " + formName, e);
      }

      JSONObject jsonForm = new JSONObject();

      // Now process each input on the form
      for(Field inputField : formField.getType().getDeclaredFields()) {
        inputField.setAccessible(true);
        String inputName = inputField.getName();

        Object value;
        try {
          value = inputField.get(formValue);
        } catch (IllegalAccessException e) {
          throw new SqoopException(ModelError.MODEL_005,
            "Issue with field " + formName + "." + inputName, e);
        }

        Input inputAnnotation = inputField.getAnnotation(Input.class);

        // Do not serialize all values
        if(inputAnnotation != null && value != null) {
          Class type = inputField.getType();

          // We need to support NULL, so we do not support primitive types
          if(type.isPrimitive()) {
            throw new SqoopException(ModelError.MODEL_007,
              "Detected primitive type " + type + " for field " + formName + "." + inputName);
          }

          if(type == String.class) {
            jsonForm.put(inputName, value);
          } else if (type.isAssignableFrom(Map.class)) {
            JSONObject map = new JSONObject();
            for(Object key : ((Map)value).keySet()) {
              map.put(key, ((Map)value).get(key));
            }
            jsonForm.put(inputName, map);
          } else if(type == Integer.class) {
            jsonForm.put(inputName, value);
          } else if(type.isEnum()) {
            jsonForm.put(inputName, value.toString());
          } else if(type == Boolean.class) {
            jsonForm.put(inputName, value);
          }else {
            throw new SqoopException(ModelError.MODEL_004,
              "Unsupported type " + type.getName() + " for input " + formName + "." + inputName);
          }
        }
      }

      jsonOutput.put(formName, jsonForm);
    }

    return jsonOutput.toJSONString();
  }

  /**
   * Parse given input JSON string and move it's values to given configuration
   * object.
   *
   * @param json JSON representation of the configuration object
   * @param configuration Configuration object to be filled
   */
  public static void fillValues(String json, Object configuration) {
    Class klass = configuration.getClass();

    JSONObject jsonForms = (JSONObject) JSONValue.parse(json);

    for(Field formField : klass.getDeclaredFields()) {
      formField.setAccessible(true);
      String formName = formField.getName();

      // We're processing only form validations
      Form formAnnotation = formField.getAnnotation(Form.class);
      if(formAnnotation == null) {
        continue;
      }

      try {
        formField.set(configuration, formField.getType().newInstance());
      } catch (Exception e) {
        throw new SqoopException(ModelError.MODEL_005,
          "Issue with field " + formName, e);
      }

      JSONObject jsonInputs = (JSONObject) jsonForms.get(formField.getName());
      if(jsonInputs == null) {
        continue;
      }

      Object formValue;
      try {
        formValue = formField.get(configuration);
      } catch (IllegalAccessException e) {
        throw new SqoopException(ModelError.MODEL_005,
          "Issue with field " + formName, e);
      }

      for(Field inputField : formField.getType().getDeclaredFields()) {
        inputField.setAccessible(true);
        String inputName = inputField.getName();

        Input inputAnnotation = inputField.getAnnotation(Input.class);

        if(inputAnnotation == null || jsonInputs.get(inputName) == null) {
          try {
            inputField.set(formValue, null);
          } catch (IllegalAccessException e) {
            throw new SqoopException(ModelError.MODEL_005,
              "Issue with field " + formName + "." + inputName, e);
          }
          continue;
        }

        Class type = inputField.getType();

        try {
          if(type == String.class) {
            inputField.set(formValue, jsonInputs.get(inputName));
          } else if (type.isAssignableFrom(Map.class)) {
            Map<String, String> map = new HashMap<String, String>();
            JSONObject jsonObject = (JSONObject) jsonInputs.get(inputName);
            for(Object key : jsonObject.keySet()) {
              map.put((String)key, (String)jsonObject.get(key));
            }
            inputField.set(formValue, map);
          } else if(type == Integer.class) {
            inputField.set(formValue, ((Long)jsonInputs.get(inputName)).intValue());
          } else if(type.isEnum()) {
            inputField.set(formValue, Enum.valueOf((Class<? extends Enum>) inputField.getType(), (String) jsonInputs.get(inputName)));
          } else if(type == Boolean.class) {
            inputField.set(formValue, (Boolean) jsonInputs.get(inputName));
          }else {
            throw new SqoopException(ModelError.MODEL_004,
              "Unsupported type " + type.getName() + " for input " + formName + "." + inputName);
          }
        } catch (IllegalAccessException e) {
          throw new SqoopException(ModelError.MODEL_005,
            "Issue with field " + formName + "." + inputName, e);
        }
      }
    }
  }

  public static String getName(Field input, Input annotation) {
    return input.getName();
  }

  public static String getName(Field form, Form annotation) {
    return form.getName();
  }

  public static ConfigurationClass getConfigurationClassAnnotation(Object object, boolean strict) {
    ConfigurationClass annotation = object.getClass().getAnnotation(ConfigurationClass.class);

    if(strict && annotation == null) {
      throw new SqoopException(ModelError.MODEL_003, "Missing annotation ConfigurationClass on class " + object.getClass().getName());
    }

    return annotation;
  }

  public static FormClass getFormClassAnnotation(Object object, boolean strict) {
    FormClass annotation = object.getClass().getAnnotation(FormClass.class);

    if(strict && annotation == null) {
      throw new SqoopException(ModelError.MODEL_003, "Missing annotation ConfigurationClass on class " + object.getClass().getName());
    }

    return annotation;
  }

  public static Form getFormAnnotation(Field field, boolean strict) {
    Form annotation = field.getAnnotation(Form.class);

    if(strict && annotation == null) {
      throw new SqoopException(ModelError.MODEL_003, "Missing annotation Form on Field " + field.getName() + " on class " + field.getDeclaringClass().getName());
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
      throw new SqoopException(ModelError.MODEL_012, e);
    }
  }

}
