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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.ConfigValidationResult;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Serialize and transfer validation results (0..N).
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ValidationResultBean implements JsonBean {

  private static final String VALIDATION_RESULT = "validation-result";
  private static final String ID = "id";
  private static final String STATUS = "status";
  private static final String MESSAGE = "message";

  private ConfigValidationResult[] results;
  private Long id;

  public ValidationResultBean() {
    // Empty, for restore
  }

  public ValidationResultBean(ConfigValidationResult ... results) {
    this.results = results;
  }

  public ConfigValidationResult[] getValidationResults() {
    return results;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getId() {
    return id;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract(boolean skipSensitive) {
    JSONArray array = new JSONArray();

    for(ConfigValidationResult result : results) {
      JSONObject output = extractValidationResult(result);
      array.add(output);
    }

    JSONObject object = new JSONObject();
    object.put(VALIDATION_RESULT, array);
    if(id != null) {
      object.put(ID, id);
    }
    return object;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractValidationResult(ConfigValidationResult result) {
    JSONObject ret = new JSONObject();
    for(Map.Entry<String, List<Message>> entry : result.getMessages().entrySet()) {
      ret.put(entry.getKey(), extractMessageList(entry.getValue()));
    }

    return ret;
  }

  @SuppressWarnings("unchecked")
  private Object extractMessageList(List<Message> messages) {
    JSONArray array = new JSONArray();

    for(Message message : messages) {
      array.add(extractMessage(message));
    }

    return array;
  }

  @SuppressWarnings("unchecked")
  private Object extractMessage(Message message) {
    JSONObject ret = new JSONObject();
    ret.put(STATUS, message.getStatus().toString());
    ret.put(MESSAGE, message.getMessage());

    return ret;
  }

  @Override
  public void restore(JSONObject jsonObject) {
    JSONArray array = (JSONArray) jsonObject.get(VALIDATION_RESULT);
    results = new ConfigValidationResult[array.size()];
    int i = 0;
    for(Object item : array) {
      results[i++] = restoreValidationResult((JSONObject) item);
    }
    if(jsonObject.containsKey(ID)) {
      id = (Long) jsonObject.get(ID);
    }
  }

  @SuppressWarnings("unchecked")
  private ConfigValidationResult restoreValidationResult(JSONObject item) {
    ConfigValidationResult result  = new ConfigValidationResult();
    Set<Map.Entry<String, JSONArray>> entrySet = item.entrySet();
    for(Map.Entry<String, JSONArray> entry : entrySet) {
      result.addMessages(entry.getKey(), restoreMessageList(entry.getValue()));
    }


    return result;
  }

  private List<Message> restoreMessageList(JSONArray array) {
    List<Message> messages = new LinkedList<Message>();
    for(Object item : array) {
      messages.add(restoreMessage((JSONObject)item));
    }
    return messages;
  }

  private Message restoreMessage(JSONObject item) {
    return new Message(
      Status.valueOf((String) item.get(STATUS)),
      (String) item.get(MESSAGE)
    );
  }
}
