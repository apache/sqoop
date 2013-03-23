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

import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

import static org.apache.sqoop.json.util.FormSerialization.*;
import static org.apache.sqoop.json.util.ResourceBundleSerialization.*;

/**
 *
 */
public class FrameworkBean implements JsonBean {


  private MFramework framework;

  private ResourceBundle bundle;

  // for "extract"
  public FrameworkBean(MFramework framework, ResourceBundle bundle) {
    this.framework = framework;
    this.bundle = bundle;
  }

  // for "restore"
  public FrameworkBean() {
  }

  public MFramework getFramework() {
    return framework;
  }

  public ResourceBundle getResourceBundle() {
    return bundle;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract(boolean skipSensitive) {
    JSONArray conForms =
      extractForms(framework.getConnectionForms().getForms(), skipSensitive);
    JSONObject jobForms = new JSONObject();

    for (MJobForms job : framework.getAllJobsForms().values()) {
      jobForms.put(job.getType().name(), extractForms(job.getForms(), skipSensitive));
    }

    JSONObject result = new JSONObject();
    result.put(ID, framework.getPersistenceId());
    result.put(CON_FORMS, conForms);
    result.put(JOB_FORMS, jobForms);
    result.put(RESOURCES, extractResourceBundle(bundle));
    return result;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void restore(JSONObject jsonObject) {
    long id = (Long) jsonObject.get(ID);

    List<MForm> connForms = restoreForms((JSONArray) jsonObject.get(CON_FORMS));

    JSONObject jobForms =  (JSONObject) jsonObject.get(JOB_FORMS);

    List<MJobForms> jobs = new ArrayList<MJobForms>();
    for( Map.Entry entry : (Set<Map.Entry>) jobForms.entrySet()) {
      //TODO(jarcec): Handle situation when server is supporting operation
      // that client do not know (server do have newer version than client)
      MJob.Type type = MJob.Type.valueOf((String) entry.getKey());

      List<MForm> job = restoreForms((JSONArray) entry.getValue());

      jobs.add(new MJobForms(type, job));
    }

    framework = new MFramework(new MConnectionForms(connForms), jobs);
    framework.setPersistenceId(id);

    bundle = restoreResourceBundle((JSONObject) jsonObject.get(RESOURCES));
  }

}
