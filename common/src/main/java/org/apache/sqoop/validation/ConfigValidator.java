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
 * Config validators.
 *
 * This class represents validations for the sqoop objects
 */
public class ConfigValidator {

  // Configuration class that belongs to this validation
  Class klass;

  // Entire validation status
  Status status;

  // Status messages for various fields
  Map<ConfigInput, Message> messages;

  public ConfigValidator(Class klass) {
    this.klass = klass;
    status = Status.getDefault();
    messages = new HashMap<ConfigInput, Message>();
  }

  public ConfigValidator(Status status, Map<ConfigInput, Message> messages) {
    this.status = status;
    this.messages = messages;
  }

  public Status getStatus() {
    return status;
  }

  public Map<ConfigInput, Message> getMessages() {
    return messages;
  }

  /**
   * Add message to config.
   *
   * @param status Severity of the message
   * @param config Config name, must be defined in the class
   * @param message Validation message
   */
  public void addMessage(Status status, String config, String message) {
    addMessage(status, config, null, message);
  }

  /**
   * Add message to input in one of the configs.
   *
   * @param status Severity of the message
   * @param config Config name, must be defined in the class
   * @param input Field name, must be defined in the config class
   * @param message Validation message
   */
  public void addMessage(Status status, String config, String input, String message ) {
    if( klass == null) {
      throw new SqoopException(ConfigValidationError.VALIDATION_0001);
    }

    assert config != null;
    assert message != null;

    // Field for specified config
    Field configField;

    // Load the config field and verify that it exists
    try {
      configField = klass.getDeclaredField(config);
    } catch (NoSuchFieldException e) {
      throw new SqoopException(ConfigValidationError.VALIDATION_0002,
        "Can't get config " + config + " from " + klass.getName(), e);
    }

    // If this is config message, just save the message and continue
    if(input == null) {
      setMessage(status, config, input, message);
      return;
    }

    // Verify that specified input exists on the config
    try {
      configField.getType().getDeclaredField(input);
    } catch (NoSuchFieldException e) {
      throw new SqoopException(ConfigValidationError.VALIDATION_0002,
        "Can't get input " + input + " from config" + configField.getType().getName(), e);
    }

    setMessage(status, config, input, message);
  }

  private void setMessage(Status status, String config, String input, String message) {
    this.status = Status.getWorstStatus(this.status, status);
    messages.put(new ConfigInput(config, input), new Message(status, message));
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

  public static class ConfigInput{
    private String config;
    private String input;

    public ConfigInput(String config, String input) {
      this.config = config;
      this.input = input;
    }

    public ConfigInput(String configInput) {
      assert configInput != null;
      String []parts = configInput.split("\\.");

      if(configInput.isEmpty() || (parts.length != 1 && parts.length != 2)) {
        throw new SqoopException(ConfigValidationError.VALIDATION_0003,
          "Specification " + configInput + " is not in valid configat config.input");
      }

      this.config = parts[0];
      if(parts.length == 2) {
        this.input = parts[1];
      }
    }

    public String getConfig() {
      return config;
    }

    public String getInput() {
      return input;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ConfigInput configInput = (ConfigInput) o;

      if (config != null ? !config.equals(configInput.config) : configInput.config != null)
        return false;
      if (input != null ? !input.equals(configInput.input) : configInput.input != null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = config != null ? config.hashCode() : 0;
      result = 31 * result + (input != null ? input.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      if(input == null) {
        return config;
      }

      return config + "." + input;
    }
  }
}
