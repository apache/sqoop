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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFormType;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ConnectorBean implements JsonBean {

  public static final String ID = "id";
  public static final String NAME = "name";
  public static final String CLASS = "class";
  public static final String CON_FORMS = "con_forms";
  public static final String JOB_FORMS = "job_forms";

  public static final String FORM_NAME = "name";
  public static final String FORM_TYPE = "type";
  public static final String FORM_INPUTS = "inputs";
  public static final String FORM_INPUT_NAME = "name";
  public static final String FORM_INPUT_TYPE = "type";
  public static final String FORM_INPUT_MASK = "mask";
  public static final String FORM_INPUT_SIZE = "size";

  private MConnector[] connectors;

  // for "extract"
  public ConnectorBean(MConnector[] connectors) {
    this.connectors = new MConnector[connectors.length];
    System.arraycopy(connectors, 0, this.connectors, 0, connectors.length);
  }

  // for "restore"
  public ConnectorBean() {
  }

  public MConnector[] getConnectors() {
    return connectors;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract() {
    JSONArray idArray = new JSONArray();
    JSONArray nameArray = new JSONArray();
    JSONArray classArray = new JSONArray();
    JSONArray conFormsArray = new JSONArray();
    JSONArray jobFormsArray = new JSONArray();

    for (MConnector connector : connectors) {
      idArray.add(connector.getPersistenceId());
      nameArray.add(connector.getUniqueName());
      classArray.add(connector.getClassName());
      conFormsArray.add(extractForms(connector.getConnectionForms().getForms()));

      JSONObject jobForms = new JSONObject();
      for (MJobForms job : connector.getAllJobsForms().values()) {
        jobForms.put(job.getType().name(), extractForms(job.getForms()));
      }
      jobFormsArray.add(jobForms);
    }

    JSONObject result = new JSONObject();
    result.put(ID, idArray);
    result.put(NAME, nameArray);
    result.put(CLASS, classArray);
    result.put(CON_FORMS, conFormsArray);
    result.put(JOB_FORMS, jobFormsArray);
    return result;
  }

  @SuppressWarnings("unchecked")
  private JSONArray extractForms(List<MForm> mForms) {
    JSONArray forms = new JSONArray();

    for (MForm mForm : mForms) {
      forms.add(extractForm(mForm));
    }

    return forms;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractForm(MForm mForm) {
    JSONObject form = new JSONObject();
    form.put(FORM_NAME, mForm.getName());
    form.put(FORM_TYPE, MFormType.CONNECTION.toString());
    JSONArray mInputs = new JSONArray();
    form.put(FORM_INPUTS, mInputs);

    for (MInput<?> mInput : mForm.getInputs()) {
      JSONObject input = new JSONObject();
      mInputs.add(input);

      input.put(FORM_INPUT_NAME, mInput.getName());
      input.put(FORM_INPUT_TYPE, mInput.getType().toString());
      if (mInput.getType() == MInputType.STRING) {
        input.put(FORM_INPUT_MASK,
            ((MStringInput)mInput).isMasked());
        input.put(FORM_INPUT_SIZE,
            ((MStringInput)mInput).getMaxLength());
      }
    }

    return form;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void restore(JSONObject jsonObject) {
    JSONArray idArray = (JSONArray) jsonObject.get(ID);
    JSONArray nameArray = (JSONArray) jsonObject.get(NAME);
    JSONArray classArray = (JSONArray) jsonObject.get(CLASS);
    JSONArray conFormsArray =
        (JSONArray) jsonObject.get(CON_FORMS);
    JSONArray jobFormsArray =
        (JSONArray) jsonObject.get(JOB_FORMS);

    connectors = new MConnector[idArray.size()];
    for (int i = 0; i < connectors.length; i++) {
      long persistenceId = (Long) idArray.get(i);
      String uniqueName = (String) nameArray.get(i);
      String className = (String) classArray.get(i);

      List<MForm> connForms = restoreForms((JSONArray) conFormsArray.get(i));

      JSONObject jobJson = (JSONObject) jobFormsArray.get(i);
      List<MJobForms> jobs = new ArrayList<MJobForms>();
      for( Map.Entry entry : (Set<Map.Entry>) jobJson.entrySet()) {
        //TODO(jarcec): Handle situation when server is supporting operation
        // that client do not know (server do have newer version than client)
        MJob.Type type = MJob.Type.valueOf((String) entry.getKey());

        List<MForm> jobForms =
          restoreForms((JSONArray) jobJson.get(entry.getKey()));

        jobs.add(new MJobForms(type, jobForms));
      }

      MConnector connector = new MConnector(uniqueName, className,
        new MConnectionForms(connForms), jobs);
      connector.setPersistenceId(persistenceId);
      connectors[i] = connector;
    }
  }

  private List<MForm> restoreForms(JSONArray forms) {
    List<MForm> mForms = new ArrayList<MForm>();

    for (int i = 0; i < forms.size(); i++) {
      mForms.add(restoreForm((JSONObject) forms.get(i)));
    }

    return mForms;
  }

  private MForm restoreForm(JSONObject form) {
    JSONArray inputs = (JSONArray) form.get(FORM_INPUTS);

    List<MInput<?>> mInputs = new ArrayList<MInput<?>>();
    for (int i = 0; i < inputs.size(); i++) {
      JSONObject input = (JSONObject) inputs.get(i);
      MInputType type =
          MInputType.valueOf((String) input.get(FORM_INPUT_TYPE));
      switch (type) {
        case STRING: {
          String name = (String) input.get(FORM_INPUT_NAME);
          boolean mask = (Boolean) input.get(FORM_INPUT_MASK);
          long size = (Long) input.get(FORM_INPUT_SIZE);
          MInput<String> mInput = new MStringInput(name, mask, (short)size);
          mInputs.add(mInput);
          break;
        }
        case MAP: {
          String name = (String) input.get(FORM_INPUT_NAME);
          MInput<Map<String, String>> mInput = new MMapInput(name);
          mInputs.add(mInput);
          break;
        }
      }
    }

    return new MForm((String) form.get(FORM_NAME), mInputs);
  }
}
