/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.connector.sftp;

import org.apache.sqoop.common.ErrorCode;

/**
 * Error messages for SFTP connector.
 */
public enum SftpConnectorError implements ErrorCode {
  SFTP_CONNECTOR_0000("Unknown error occurred."),
  SFTP_CONNECTOR_0001("Error occurred connecting to SFTP server."),
  SFTP_CONNECTOR_0002("Error occurred disconnecting from SFTP server."),
  SFTP_CONNECTOR_0003("Error occurred transferring data to SFTP server."),
  SFTP_CONNECTOR_0004("Unknown job type")
  ;

  private final String message;

  private SftpConnectorError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }

}
