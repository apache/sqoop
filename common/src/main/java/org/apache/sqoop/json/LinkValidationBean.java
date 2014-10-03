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

import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.ConfigValidator;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Bean for sending validations across network.This bean will transfer link config
 * validation results. Optionally validation bean can also transfer
 * created persistent id in case that new entity was created.
 */
public class LinkValidationBean implements JsonBean {

  private static final String ID = "id";
  private static final String STATUS = "status";
  private static final String MESSAGE = "message";
  private static final String MESSAGES = "messages";

  private Long id;
  private ConfigValidator linkConfigValidation;

  // For "extract"
  public LinkValidationBean(ConfigValidator linkConfigValidator) {
    this();

    this.linkConfigValidation = linkConfigValidator;
  }

  // For "restore"
  public LinkValidationBean() {
    id = null;
  }

  public ConfigValidator getLinkConfigValidator() {
    return linkConfigValidation;
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

    // Optionally transfer id
    if(id != null) {
      object.put(ID, id);
    }
    object.put(LinkBean.LINK_CONFIG, extractValidation(linkConfigValidation));
    return object;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractValidation(ConfigValidator validation) {
    JSONObject object = new JSONObject();

    object.put(STATUS, validation.getStatus().name());

    JSONObject jsonMessages = new JSONObject();
    Map<ConfigValidator.ConfigInput, ConfigValidator.Message> messages = validation.getMessages();

    for(Map.Entry<ConfigValidator.ConfigInput, ConfigValidator.Message> entry : messages.entrySet()) {
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

    linkConfigValidation = restoreValidation(
      (JSONObject)jsonObject.get(LinkBean.LINK_CONFIG));
  }

  public ConfigValidator restoreValidation(JSONObject jsonObject) {
    JSONObject jsonMessages = (JSONObject) jsonObject.get(MESSAGES);
    Map<ConfigValidator.ConfigInput, ConfigValidator.Message> messages
      = new HashMap<ConfigValidator.ConfigInput, ConfigValidator.Message>();

    for(Object key : jsonMessages.keySet()) {
      JSONObject jsonMessage = (JSONObject) jsonMessages.get(key);

      Status status = Status.valueOf((String) jsonMessage.get(STATUS));
      String stringMessage = (String) jsonMessage.get(MESSAGE);

      ConfigValidator.Message message
        = new ConfigValidator.Message(status, stringMessage);

      messages.put(new ConfigValidator.ConfigInput((String)key), message);
    }

    Status status = Status.valueOf((String) jsonObject.get(STATUS));

    return new ConfigValidator(status, messages);
  }
}
