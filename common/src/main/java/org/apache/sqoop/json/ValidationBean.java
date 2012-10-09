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

import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.model.MValidatedElement;
import org.apache.sqoop.validation.Status;
import org.json.simple.JSONObject;

import java.util.List;

/**
 * Bean for sending validations across network. As is expected that both filled
 * forms will be available on both ends (client and server), we're transferring
 * only validation status and messages.
 */
public class ValidationBean implements JsonBean {

  private static final String STATUS = "status";
  private static final String TYPE = "type";
  private static final String ID = "id";
  private static final String CONNECTOR_PART = "connector";
  private static final String FRAMEWORK_PART = "framework";

  private static final String SEVERITY = "severity";
  private static final String MESSAGE = "message";

  private MConnection connection;
  private MJob job;
  private Status status;

  // For "extract"
  public ValidationBean(MConnection connection, Status status) {
    this.connection = connection;
    this.status = status;
  }
  public ValidationBean(MJob job, Status status) {
    this.job = job;
    this.status = status;
  }

  // For "restore"
  public ValidationBean(MConnection connection) {
    this.connection = connection;
  }
  public ValidationBean(MJob job) {
    this.job = job;
  }

  public MConnection getConnection() {
    return connection;
  }
  public MJob getJob() {
    return job;
  }

  public Status getStatus() {
    return status;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract() {
    JSONObject object = new JSONObject();

    List<MForm> connectorPart = null;
    List<MForm> frameworkPart = null;

    if(connection != null) {
      connectorPart = connection.getConnectorPart().getForms();
      frameworkPart = connection.getFrameworkPart().getForms();
      object.put(TYPE, "CONNECTION");
    } else if (job != null) {
      connectorPart = job.getConnectorPart().getForms();
      frameworkPart = job.getFrameworkPart().getForms();
      object.put(TYPE, "FRAMEWORK");
    } else {
      // This should never happen
      return null;
    }


    object.put(STATUS, status.name());
    object.put(CONNECTOR_PART, extractForms(connectorPart));
    object.put(FRAMEWORK_PART, extractForms(frameworkPart));

    // If we do have ID available, let's send it across network
    long id = MPersistableEntity.PERSISTANCE_ID_DEFAULT;
    if(connection != null) {
      id = connection.getPersistenceId();
    } else if(job != null) {
      id = job.getPersistenceId();
    }
    if( id != MPersistableEntity.PERSISTANCE_ID_DEFAULT) {
      object.put(ID, id);
    }

    return object;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractForms(List<MForm> forms) {
    JSONObject ret = new JSONObject();

    for (MForm form : forms) {
      ret.put(form.getPersistenceId(), extractForm(form));
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractForm(MForm form) {
    JSONObject object = new JSONObject();

    for (MInput input : form.getInputs()) {
      if (input.getValidationSeverity() != MValidatedElement.Severity.OK) {
        JSONObject validation = new JSONObject();
        validation.put(SEVERITY, input.getValidationSeverity().name());
        validation.put(MESSAGE, input.getValidationMessage());

        object.put(input.getPersistenceId(), validation);
      }
    }

    return object;
  }

  @Override
  public void restore(JSONObject jsonObject) {
    status = Status.valueOf((String) jsonObject.get(STATUS));

    JSONObject connectorPart = (JSONObject) jsonObject.get(CONNECTOR_PART);
    JSONObject frameworkPart = (JSONObject) jsonObject.get(FRAMEWORK_PART);

    if(connection != null) {
      restoreForms(connectorPart, connection.getConnectorPart().getForms());
      restoreForms(frameworkPart, connection.getFrameworkPart().getForms());
    } else if (job != null) {
      restoreForms(connectorPart, job.getConnectorPart().getForms());
      restoreForms(frameworkPart, job.getFrameworkPart().getForms());
    }

    // Restore persistent id if available
    if(jsonObject.containsKey(ID)) {
      long id = (Long)jsonObject.get(ID);
      if(connection != null) {
        connection.setPersistenceId(id);
      } else if(job != null) {
        job.setPersistenceId(id);
      }
    }
  }

  private void restoreForms(JSONObject json, List<MForm> forms) {
    for (MForm form : forms) {
      String id = Long.toString(form.getPersistenceId());
      if (json.containsKey(id)) {
        restoreForm((JSONObject) json.get(id), form);
      }
    }
  }

  private void restoreForm(JSONObject json, MForm form) {
    for (MInput input : form.getInputs()) {
      String id = Long.toString(input.getPersistenceId());
      if (json.containsKey(id)) {
        JSONObject validation = (JSONObject) json.get(id);

        MValidatedElement.Severity severity =
          MValidatedElement.Severity.valueOf((String) validation.get(SEVERITY));
        String message = (String) validation.get(MESSAGE);

        input.setValidationMessage(severity, message);
      } else {
        input.setValidationMessage(MValidatedElement.Severity.OK, null);
      }
    }
  }

}
