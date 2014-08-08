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

import org.apache.sqoop.common.ConnectorType;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Bean for sending validations across network. This bean will move two
 * validation objects at one time - one for connector and second for framework
 * part of validated entity. Optionally validation bean can also transfer
 * created persistent id in case that new entity was created.
 */
public class JobValidationBean implements JsonBean {

  private static final String ID = "id";
  private static final String FRAMEWORK = "framework";
  private static final String CONNECTOR = "connector";
  private static final String FROM = "from";
  private static final String TO = "to";
  private static final String STATUS = "status";
  private static final String MESSAGE = "message";
  private static final String MESSAGES = "messages";

  private Long id;
  private Map<ConnectorType, Validation> connectorValidation;
  private Validation frameworkValidation;

  // For "extract"
  public JobValidationBean(Validation fromConnector, Validation framework, Validation toConnector) {
    this();

    this.connectorValidation = new HashMap<ConnectorType, Validation>();
    this.connectorValidation.put(ConnectorType.FROM, fromConnector);
    this.connectorValidation.put(ConnectorType.TO, toConnector);
    this.frameworkValidation = framework;
  }

  // For "restore"
  public JobValidationBean() {
    id = null;
    connectorValidation = new HashMap<ConnectorType, Validation>();
  }

  public Validation getConnectorValidation(ConnectorType type) {
    return connectorValidation.get(type);
  }

  public Validation getFrameworkValidation() {
    return frameworkValidation;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getId() {
    return id;
  }

  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONObject object = new JSONObject();
    JSONObject connectorObject = new JSONObject();

    // Optionally transfer id
    if(id != null) {
      object.put(ID, id);
    }

    connectorObject.put(FROM, extractValidation(getConnectorValidation(ConnectorType.FROM)));
    connectorObject.put(TO, extractValidation(getConnectorValidation(ConnectorType.TO)));

    object.put(FRAMEWORK, extractValidation(frameworkValidation));
    object.put(CONNECTOR, connectorObject);

    return object;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractValidation(Validation validation) {
    JSONObject object = new JSONObject();

    object.put(STATUS, validation.getStatus().name());

    JSONObject jsonMessages = new JSONObject();
    Map<Validation.FormInput, Validation.Message> messages = validation.getMessages();

    for(Map.Entry<Validation.FormInput, Validation.Message> entry : messages.entrySet()) {
      JSONObject jsonEntry = new JSONObject();
      jsonEntry.put(STATUS, entry.getValue().getStatus().name());
      jsonEntry.put(MESSAGE, entry.getValue().getMessage());
      jsonMessages.put(entry.getKey(), jsonEntry);
    }

    object.put(MESSAGES, jsonMessages);

    return object;
  }

  @Override
  public void restore(JSONObject jsonObject) {
    // Optional and accepting NULLs
    id = (Long) jsonObject.get(ID);

    JSONObject jsonConnectorObject = (JSONObject)jsonObject.get(CONNECTOR);

    connectorValidation.put(ConnectorType.FROM, restoreValidation(
        (JSONObject)jsonConnectorObject.get(FROM)));
    connectorValidation.put(ConnectorType.TO, restoreValidation(
        (JSONObject)jsonConnectorObject.get(TO)));
    frameworkValidation = restoreValidation(
        (JSONObject)jsonObject.get(FRAMEWORK));
  }

  public Validation restoreValidation(JSONObject jsonObject) {
    JSONObject jsonMessages = (JSONObject) jsonObject.get(MESSAGES);
    Map<Validation.FormInput, Validation.Message> messages
        = new HashMap<Validation.FormInput, Validation.Message>();

    for(Object key : jsonMessages.keySet()) {
      JSONObject jsonMessage = (JSONObject) jsonMessages.get(key);

      Status status = Status.valueOf((String) jsonMessage.get(STATUS));
      String stringMessage = (String) jsonMessage.get(MESSAGE);

      Validation.Message message
          = new Validation.Message(status, stringMessage);

      messages.put(new Validation.FormInput((String)key), message);
    }

    Status status = Status.valueOf((String) jsonObject.get(STATUS));

    return new Validation(status, messages);
  }
}
