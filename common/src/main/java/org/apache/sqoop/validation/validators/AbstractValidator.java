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
package org.apache.sqoop.validation.validators;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;

import java.util.LinkedList;
import java.util.List;

/**
 * Abstract validator class.
 *
 * Can be used to validate inputs, forms and configuration classes.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
abstract public class AbstractValidator<T> {

  /**
   * Default value of String argument.
   */
  public static final String DEFAULT_STRING_ARGUMENT = "";

  /**
   * Validation check.
   *
   * To be implemented by our children.
   *
   * @param instance Object to validate (depending on what we are validating)
   */
  abstract public void validate(T instance);

  /**
   * Messages generated during validation.
   */
  private List<Message> messages;

  /**
   * Overall status of the validation.
   */
  private Status status;

  /**
   * Optional String argument that can be defined for the Validator
   */
  private String stringArgument;

  public AbstractValidator() {
    reset();
  }

  protected void addMessage(Message msg) {
    status = Status.getWorstStatus(status, msg.getStatus());
    messages.add(msg);
  }

  protected void addMessage(Status status, String msg) {
    addMessage(new Message(status, msg));
  }

  public List<Message> getMessages() {
    return messages;
  }

  public Status getStatus() {
    return status;
  }

  public void setStringArgument(String arg) {
    this.stringArgument = arg;
  }

  public String getStringArgument() {
    return stringArgument;
  }

  /**
   * Reset validator state.
   *
   * * Previous messages
   * * Status
   * * Any stored arguments
   */
  public void reset() {
    messages = new LinkedList<Message>();
    status = Status.getDefault();
    stringArgument = DEFAULT_STRING_ARGUMENT;
  }
}
