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
package org.apache.sqoop.error.code;

import org.apache.sqoop.common.ErrorCode;

public enum KafkaConnectorErrors implements ErrorCode {

  KAFKA_CONNECTOR_0000("Unknown error occurred."),
  KAFKA_CONNECTOR_0001("Error occurred while sending data to Kafka")
  ;

  private final String message;

  private KafkaConnectorErrors(String message) {
    this.message = message;
  }

  @Override
  public String getCode() {
    return name();
  }

  /**
   * @return the message associated with error code.
   */
  @Override
  public String getMessage() {
    return message;
  }
}
