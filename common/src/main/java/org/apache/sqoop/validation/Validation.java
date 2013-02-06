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

import org.apache.sqoop.common.SqoopException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Validation class.
 *
 * This class represents validations to given configuration object.
 */
public class Validation {

  // Configuration class that belongs to this validation
  Class klass;

  // Entire validation status
  Status status;

  // Status messages for various fields
  Map<FormInput, Message> messages;

  public Validation(Class klass) {
    this.klass = klass;
    status = Status.getDefault();
    messages = new HashMap<FormInput, Message>();
  }

  public Validation(Status status, Map<FormInput, Message> messages) {
    this.status = status;
    this.messages = messages;
  }

  public Status getStatus() {
    return status;
  }

  public Map<FormInput, Message> getMessages() {
    return messages;
  }

  /**
   * Add message to form.
   *
   * @param status Severity of the message
   * @param form Form name, must be defined in the class
   * @param message Validation message
   */
  public void addMessage(Status status, String form, String message) {
    addMessage(status, form, null, message);
  }

  /**
   * Add message to input in one of the forms.
   *
   * @param status Severity of the message
   * @param form Form name, must be defined in the class
   * @param input Field name, must be defined in the form class
   * @param message Validation message
   */
  public void addMessage(Status status, String form, String input, String message ) {
    if( klass == null) {
      throw new SqoopException(ValidationError.VALIDATION_0001);
    }

    assert form != null;
    assert message != null;

    // Field for specified form
    Field formField;

    // Load the form field and verify that it exists
    try {
      formField = klass.getDeclaredField(form);
    } catch (NoSuchFieldException e) {
      throw new SqoopException(ValidationError.VALIDATION_0002,
        "Can't get form " + form + " from " + klass.getName(), e);
    }

    // If this is form message, just save the message and continue
    if(input == null) {
      setMessage(status, form, input, message);
      return;
    }

    // Verify that specified input exists on the form
    try {
      formField.getType().getDeclaredField(input);
    } catch (NoSuchFieldException e) {
      throw new SqoopException(ValidationError.VALIDATION_0002,
        "Can't get input " + input + " from form" + formField.getType().getName(), e);
    }

    setMessage(status, form, input, message);
  }

  private void setMessage(Status status, String form, String input, String message) {
    this.status = Status.getWorstStatus(this.status, status);
    messages.put(new FormInput(form, input), new Message(status, message));
  }

  public static class Message {
    private Status status;
    private String message;

    public Message(Status status, String message) {
      this.status = status;
      this.message = message;
    }

    public Status getStatus() {
      return status;
    }

    public String getMessage() {
      return message;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Message)) return false;

      Message message1 = (Message) o;

      if (message != null ? !message.equals(message1.message) : message1.message != null)
        return false;
      if (status != message1.status) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = status != null ? status.hashCode() : 0;
      result = 31 * result + (message != null ? message.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "{" + status.name() + ": " + message + "}";
    }
  }

  public static class FormInput {
    private String form;
    private String input;

    public FormInput(String form, String input) {
      this.form = form;
      this.input = input;
    }

    public FormInput(String formInput) {
      assert formInput != null;
      String []parts = formInput.split("\\.");

      if(formInput.isEmpty() || (parts.length != 1 && parts.length != 2)) {
        throw new SqoopException(ValidationError.VALIDATION_0003,
          "Specification " + formInput + " is not in valid format form.input");
      }

      this.form = parts[0];
      if(parts.length == 2) {
        this.input = parts[1];
      }
    }

    public String getForm() {
      return form;
    }

    public String getInput() {
      return input;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      FormInput formInput = (FormInput) o;

      if (form != null ? !form.equals(formInput.form) : formInput.form != null)
        return false;
      if (input != null ? !input.equals(formInput.input) : formInput.input != null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = form != null ? form.hashCode() : 0;
      result = 31 * result + (input != null ? input.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      if(input == null) {
        return form;
      }

      return form + "." + input;
    }
  }
}
