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
package org.apache.sqoop.validation;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.validation.validators.AbstractValidator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Result of validation execution.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ConfigValidationResult {

  /**
   * All messages for each named item.
   */
  Map<String, List<Message>> messages;

  /**
   * Overall status.
   */
  Status status;

  public ConfigValidationResult() {
    messages = new HashMap<String, List<Message>>();
    status = Status.getDefault();
  }

  /**
   * Add given validator result to this instance.
   *
   * @param name Full name of the validated object
   * @param validator Executed validator
   */
  public void addValidatorResult(String name, AbstractValidator<String> validator) {
    if(validator.getStatus() == Status.getDefault()) {
      return;
    }

    status = Status.getWorstStatus(status, validator.getStatus());
    if(messages.containsKey(name)) {
     messages.get(name).addAll(validator.getMessages());
    } else {
      messages.put(name, validator.getMessages());
    }
  }

  /**
   * Merge results with another validation result.
   *
   * @param result Other validation result
   */
  public void mergeValidatorResult(ConfigValidationResult result) {
    messages.putAll(result.messages);
    status = Status.getWorstStatus(status, result.status);
  }

  /**
   * Method to directly add messages for given name.
   *
   * This method will replace previous messages for given name.
   *
   * @param name Name of the entity
   * @param messages List of messages associated with the name
   */
  public void addMessages(String name, List<Message> messages) {
    this.messages.put(name, messages);

    for(Message message : messages) {
      this.status = Status.getWorstStatus(status, message.getStatus());
    }
  }

  public Status getStatus() {
    return status;
  }

  public Map<String, List<Message>> getMessages() {
    return messages;
  }
}
