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
package org.apache.sqoop.model;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;

import java.util.LinkedList;
import java.util.List;

/**
 * Element that can have associated validation messages (0..N).
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class MValidatedElement extends MNamedElement {

  /**
   * Validation messages.
   */
  private List<Message> validationMessages;

  /**
   * The worst status of all validation messages.
   */
  private Status validationStatus;

  public MValidatedElement(String name) {
    super(name);
    resetValidationMessages();
  }

  public MValidatedElement(MValidatedElement other) {
    super(other);
    resetValidationMessages();
    this.validationStatus = other.validationStatus;
    this.validationMessages.addAll(other.validationMessages);
  }

  /**
   * Reset this validated element back to default state.
   *
   * Will remove all associated messages and validation status.
   */
  public void resetValidationMessages() {
    this.validationStatus = Status.getDefault();
    this.validationMessages = new LinkedList<Message>();
  }

  /**
   * Set validation messages (override anything that has been set before).
   *
   * @param msg Message itself
   */
  public void addValidationMessage(Message msg) {
    this.validationMessages.add(msg);
    this.validationStatus = Status.getWorstStatus(this.validationStatus, msg.getStatus());
  }

  /**
   * Override all previously existing validation messages.
   *
   * @param messages
   */
  public void setValidationMessages(List<Message> messages) {
    this.validationMessages = messages;
    this.validationStatus = Status.getDefault();

    for(Message message : messages ) {
      this.validationStatus = Status.getWorstStatus(this.validationStatus, message.getStatus());
    }
  }

  /**
   * Return validation message for given severity.
   *
   * Return either associated message for given severity or null in case
   * that there is no message with given severity.
   */
  public List<Message> getValidationMessages() {
    return this.validationMessages;
  }

  /**
   * Return message validation status.
   */
  public Status getValidationStatus() {
    return validationStatus;
  }

}
