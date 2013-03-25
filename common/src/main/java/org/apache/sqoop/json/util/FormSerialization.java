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

import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFormType;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.utils.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Convenient static methods for serializing forms.
 */
public final class FormSerialization {

  public static final String ALL = "all";
  public static final String ID = "id";
  public static final String NAME = "name";
  public static final String VERSION = "version";
  public static final String CLASS = "class";
  public static final String CREATED = "created";
  public static final String UPDATED = "updated";
  public static final String CON_FORMS = "con-forms";
  public static final String JOB_FORMS = "job-forms";

  public static final String FORM_NAME = "name";
  public static final String FORM_TYPE = "type";
  public static final String FORM_INPUTS = "inputs";
  public static final String FORM_INPUT_NAME = "name";
  public static final String FORM_INPUT_TYPE = "type";
  public static final String FORM_INPUT_SENSITIVE = "sensitive";
  public static final String FORM_INPUT_SIZE = "size";
  public static final String FORM_INPUT_VALUE = "value";
  public static final String FORM_INPUT_VALUES = "values";

  /**
   * Transform given list of forms to JSON Array object.
   *
   * @param mForms List of forms.
   * @return JSON object with serialized form of the list.
   */
  @SuppressWarnings("unchecked")
  public static JSONArray extractForms(List<MForm> mForms, boolean skipSensitive) {
    JSONArray forms = new JSONArray();

    for (MForm mForm : mForms) {
      forms.add(extractForm(mForm, skipSensitive));
    }

    return forms;
  }

  /**
   * Transform given form to JSON Object.
   *
   * @param mForm Given MForm instance
   * @param skipSensitive conditionally add sensitive input values
   * @return Serialized JSON object.
   */
  @SuppressWarnings("unchecked")
  public static JSONObject extractForm(MForm mForm, boolean skipSensitive) {
    JSONObject form = new JSONObject();
    form.put(ID, mForm.getPersistenceId());
    form.put(FORM_NAME, mForm.getName());
    form.put(FORM_TYPE, MFormType.CONNECTION.toString());
    JSONArray mInputs = new JSONArray();
    form.put(FORM_INPUTS, mInputs);

    for (MInput<?> mInput : mForm.getInputs()) {
      JSONObject input = new JSONObject();
      input.put(ID, mInput.getPersistenceId());
      input.put(FORM_INPUT_NAME, mInput.getName());
      input.put(FORM_INPUT_TYPE, mInput.getType().toString());
      input.put(FORM_INPUT_SENSITIVE, mInput.isSensitive());

      // String specific serialization
      if (mInput.getType() == MInputType.STRING) {
        input.put(FORM_INPUT_SIZE,
            ((MStringInput)mInput).getMaxLength());
      }

      // Enum specific serialization
      if(mInput.getType() == MInputType.ENUM) {
        input.put(FORM_INPUT_VALUES,
          StringUtils.join(((MEnumInput)mInput).getValues(), ","));
      }

      // Serialize value if is there
      // Skip if sensitive
      if (!mInput.isEmpty() && !(skipSensitive && mInput.isSensitive())) {
        input.put(FORM_INPUT_VALUE, mInput.getUrlSafeValueString());
      }

      mInputs.add(input);
    }

    return form;
  }

  /**
   * Restore List of MForms from JSON Array.
   *
   * @param forms JSON array representing list of MForms
   * @return Restored list of MForms
   */
  public static List<MForm> restoreForms(JSONArray forms) {
    List<MForm> mForms = new ArrayList<MForm>();

    for (int i = 0; i < forms.size(); i++) {
      mForms.add(restoreForm((JSONObject) forms.get(i)));
    }

    return mForms;
  }

  /**
   * Restore one MForm from JSON Object.
   *
   * @param form JSON representation of the MForm.
   * @return Restored MForm.
   */
  public static MForm restoreForm(JSONObject form) {
    JSONArray inputs = (JSONArray) form.get(FORM_INPUTS);

    List<MInput<?>> mInputs = new ArrayList<MInput<?>>();
    for (int i = 0; i < inputs.size(); i++) {
      JSONObject input = (JSONObject) inputs.get(i);
      MInputType type =
          MInputType.valueOf((String) input.get(FORM_INPUT_TYPE));
      String name = (String) input.get(FORM_INPUT_NAME);
      Boolean sensitive = (Boolean) input.get(FORM_INPUT_SENSITIVE);
      MInput mInput = null;
      switch (type) {
        case STRING: {
          long size = (Long) input.get(FORM_INPUT_SIZE);
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
        case ENUM: {
          String values = (String) input.get(FORM_INPUT_VALUES);
          mInput = new MEnumInput(name, sensitive.booleanValue(), values.split(","));
          break;
        }
      }

      // Propagate form ID
      mInput.setPersistenceId((Long)input.get(ID));

      // Propagate form optional value
      if(input.containsKey(FORM_INPUT_VALUE)) {
        mInput.restoreFromUrlSafeValueString(
          (String) input.get(FORM_INPUT_VALUE));
      }
      mInputs.add(mInput);
    }

    MForm mForm = new MForm((String) form.get(FORM_NAME), mInputs);
    mForm.setPersistenceId((Long) form.get(ID));
    return mForm;
  }

  private FormSerialization() {
    // Do not instantiate
  }
}
