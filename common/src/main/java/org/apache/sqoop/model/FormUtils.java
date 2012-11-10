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
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Util class for transforming data from correctly annotated configuration
 * objects to form structures and vice-versa.
 */
public class FormUtils {

  /**
   * Transform correctly annotated configuration object to corresponding
   * list of forms.
   *
   * Forms will be order according to the first occurrence in the configuration
   * class. Inputs will be also ordered based on occurrence, however
   * interleaving of inputs from different forms is perfectly fine.
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
    Configuration global =
      (Configuration)klass.getAnnotation(Configuration.class);

    // Each configuration object must have this class annotation
    if(global == null) {
      throw new SqoopException(ModelError.MODEL_003,
        "Missing annotation Configuration on class " + klass.getName());
    }

    // List of forms in order they appeared in configuration class
    List<String> forms = new LinkedList<String>();

    // Intermediate list of inputs for each defined form
    Map<String, List<MInput<?>>> formInputs
      = new HashMap<String, List<MInput<?>>>();

    // Iterate over all declared fields
    for (Field field : klass.getDeclaredFields()) {
      field.setAccessible(true);

      String fieldName = field.getName();

      // Each field that should be part of user input should have Input
      // annotation.
      Input inputAnnotation = field.getAnnotation(Input.class);

      if(inputAnnotation != null) {
        String form = inputAnnotation.form();
        boolean sensitive = inputAnnotation.sensitive();
        short maxLen = inputAnnotation.size();
        Class type = field.getType();

        // Make sure that we will create new entries for newly discovered forms
        if(!forms.contains(form)) {
          forms.add(form);
          formInputs.put(form, new LinkedList<MInput<?>>());
        }

        MInput input;

        // We need to support NULL, so we do not support primitive types
        if(type.isPrimitive()) {
          throw new SqoopException(ModelError.MODEL_007,
            "Detected primitive type " + type + " for field " + fieldName);
        }

        // Instantiate corresponding MInput<?> structure
        if(type == String.class) {
          input = new MStringInput(fieldName, sensitive, maxLen);
        } else if (type.isAssignableFrom(Map.class)) {
          input = new MMapInput(fieldName);
        } else if(type == Integer.class) {
          input = new MIntegerInput(fieldName);
        } else if(type.isEnum()) {
          input = new MEnumInput(fieldName, ClassUtils.getEnumStrings(type));
        } else {
          throw new SqoopException(ModelError.MODEL_004,
            "Unsupported type " + type.getName() + " for input " + fieldName);
        }

        // Move value if it's present in original configuration object
        if(configuration != null) {
          Object value;
          try {
            value = field.get(configuration);
          } catch (IllegalAccessException e) {
            throw new SqoopException(ModelError.MODEL_005,
              "Issue with field " + field.getName(), e);
          }
          if(value == null) {
            input.setEmpty();
          } else {
            input.setValue(value);
          }
        }

        formInputs.get(form).add(input);
      }
    }

    // Create corresponding forms based on our intermediate structures
    List<MForm> ret = new LinkedList<MForm>();
    for (String form : forms) {
      ret.add(new MForm(form, formInputs.get(form)));
    }

    return ret;
  }

  /**
   * Move form values from form list into corresponding configuration object.
   *
   * @param forms Input form list
   * @param configuration Output configuration object
   */
  public static void fillValues(List<MForm> forms, Object configuration) {
    Class klass = configuration.getClass();

    for(MForm form : forms) {
      for(MInput input : form.getInputs()) {
        Field field;
        try {
          field = klass.getDeclaredField(input.getName());
        } catch (NoSuchFieldException e) {
          throw new SqoopException(ModelError.MODEL_006,
            "Missing field " + input.getName(), e);
        }

        // We need to access this field even if it would be declared as private
        field.setAccessible(true);

        // Propagate value to the configuration object
        try {
          if(input.isEmpty()) {
            field.set(configuration, null);
          } else {
            if (input.getType() == MInputType.ENUM) {
              field.set(configuration, Enum.valueOf((Class<? extends Enum>)field.getType(), (String) input.getValue()));
            } else {
              field.set(configuration, input.getValue());
            }
          }
        } catch (IllegalAccessException e) {
          throw new SqoopException(ModelError.MODEL_005,
            "Issue with field " + field.getName(), e);
        }
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
    Map<String, Validation.Message> messages = validation.getMessages();

    for(MForm form : forms) {
      for(MInput input : form.getInputs()) {
        String inputName = input.getName();
        if(messages.containsKey(inputName)){
          Validation.Message message = messages.get(inputName);

          input.setValidationMessage(message.getStatus(), message.getMessage());
        } else {
          input.setValidationMessage(Status.getDefault(), null);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static String toJson(Object configuration) {
    Class klass = configuration.getClass();

    Configuration global =
      (Configuration)klass.getAnnotation(Configuration.class);

    // Each configuration object must have this class annotation
    if(global == null) {
      throw new SqoopException(ModelError.MODEL_003,
        "Missing annotation Configuration on class " + klass.getName());
    }

    JSONObject jsonObject = new JSONObject();

    // Iterate over all declared fields
    for (Field field : klass.getDeclaredFields()) {
      field.setAccessible(true);
      String fieldName = field.getName();

      // Each field that should be part of user input should have Input
      // annotation.
      Input inputAnnotation = field.getAnnotation(Input.class);

      Object value;
      try {
        value = field.get(configuration);
      } catch (IllegalAccessException e) {
        throw new SqoopException(ModelError.MODEL_005,
          "Issue with field " + field.getName(), e);
      }

      // Do not serialize all values
      if(inputAnnotation != null && value != null) {
        Class type = field.getType();

        // We need to support NULL, so we do not support primitive types
        if(type.isPrimitive()) {
          throw new SqoopException(ModelError.MODEL_007,
            "Detected primitive type " + type + " for field " + fieldName);
        }

        if(type == String.class) {
          jsonObject.put(fieldName, value);
        } else if (type.isAssignableFrom(Map.class)) {
          JSONObject map = new JSONObject();
          for(Object key : ((Map)value).keySet()) {
            map.put(key, map.get(key));
          }
          jsonObject.put(fieldName, map);
        } else if(type == Integer.class) {
          jsonObject.put(fieldName, value);
        } else if(type.isEnum()) {
          jsonObject.put(fieldName, value);
        } else {
          throw new SqoopException(ModelError.MODEL_004,
            "Unsupported type " + type.getName() + " for input " + fieldName);
        }
      }
    }

    return jsonObject.toJSONString();
  }

  // TODO(jarcec): This method currently do not iterate over all fields and
  // therefore some fields might have original values when original object will
  // be reused. This is unfortunately not acceptable.
  public static void fillValues(String json, Object configuration) {
    Class klass = configuration.getClass();

    JSONObject jsonObject = (JSONObject) JSONValue.parse(json);

    for(Object k : jsonObject.keySet()) {
      String key = (String)k;

      Field field;
      try {
        field = klass.getDeclaredField(key);
      } catch (NoSuchFieldException e) {
        throw new SqoopException(ModelError.MODEL_006,
          "Missing field " + key, e);
      }

      // We need to access this field even if it would be declared as private
      field.setAccessible(true);
      Class type = field.getType();

      try {
        if(type == String.class) {
          field.set(configuration, jsonObject.get(key));
        } else if (type.isAssignableFrom(Map.class)) {
          Map<String, String> map = new HashMap<String, String>();
          for(Object kk : jsonObject.keySet()) {
            map.put((String)kk, (String)jsonObject.get(kk));
          }
          field.set(key, map);
        } else if(type == Integer.class) {
          field.set(configuration, jsonObject.get(key));
        } else if(type == Integer.class) {
          field.set(configuration, Enum.valueOf((Class<? extends Enum>)field.getType(), (String) jsonObject.get(key)));
        } else {
          throw new SqoopException(ModelError.MODEL_004,
            "Unsupported type " + type.getName() + " for input " + key);
        }
      } catch (IllegalAccessException e) {
        throw new SqoopException(ModelError.MODEL_005,
          "Issue with field " + field.getName(), e);
      }
    }
  }

}
