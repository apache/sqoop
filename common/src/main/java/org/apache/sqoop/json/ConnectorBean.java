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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MForm;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static org.apache.sqoop.json.util.FormSerialization.*;
import static org.apache.sqoop.json.util.ResourceBundleSerialization.*;

public class ConnectorBean implements JsonBean {

  private List<MConnector> connectors;

  private List<ResourceBundle> bundles;

  // for "extract"
  public ConnectorBean(List<MConnector> connectors,
                       List<ResourceBundle> bundles) {
    this.connectors = connectors;
    this.bundles = bundles;
  }

  // for "restore"
  public ConnectorBean() {
  }

  public List<MConnector> getConnectors() {
    return connectors;
  }

  public List<ResourceBundle> getResourceBundles() {
    return bundles;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract() {
    JSONArray idArray = new JSONArray();
    JSONArray nameArray = new JSONArray();
    JSONArray classArray = new JSONArray();
    JSONArray conFormsArray = new JSONArray();
    JSONArray jobFormsArray = new JSONArray();
    JSONArray bundlesArray;

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

    bundlesArray = extractResourceBundles(bundles);

    JSONObject result = new JSONObject();
    result.put(ID, idArray);
    result.put(NAME, nameArray);
    result.put(CLASS, classArray);
    result.put(CON_FORMS, conFormsArray);
    result.put(JOB_FORMS, jobFormsArray);
    result.put(RESOURCES, bundlesArray);
    return result;
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

    connectors = new LinkedList<MConnector>();
    for (int i = 0; i < idArray.size(); i++) {
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
      connectors.add(connector);
    }

    bundles = restoreResourceBundles((JSONArray) jsonObject.get(RESOURCES));
  }
}
