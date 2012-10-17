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

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class Validation {

  // Configuration class that belongs to this validation
  Class klass;

  // Entire validation status
  Status status;

  // Status messages for various fields
  Map<String, Message> messages;

  private Validation() {
    klass = null;
  }
  public Validation(Class klass) {
    this();

    this.klass = klass;
    status = Status.getDefault();
    messages = new HashMap<String, Message>();
  }
  public Validation(Status status, Map<String, Message> messages) {
    this();

    this.status = status;
    this.messages = messages;
  }

  public Status getStatus() {
    return status;
  }

  public Map<String, Message> getMessages() {
    return messages;
  }

  public void addMessage(Status status, String field, String message ) {
    if( klass == null) {
      throw new SqoopException(ValidationError.VALIDATION_0001);
    }

    // Verify that this is valid field in configuration object
    try {
      klass.getDeclaredField(field);
    } catch (NoSuchFieldException e) {
      throw new SqoopException(ValidationError.VALIDATION_0002,
        "Field " + field + " is not present in " + klass.getName());
    }

    this.status = Status.getWorstStatus(this.status, status);
    messages.put(field, new Message(status, message));
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
}
