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
package org.apache.sqoop.shell.core;

import org.apache.sqoop.common.ErrorCode;

public enum ShellError implements ErrorCode {

  /** An unknown error has occurred. */
  SHELL_0000("An unknown error has occurred"),

  /** The specified command is not recognized. */
  SHELL_0001("The specified command is not recognized"),

  /** The specified function is not recognized. */
  SHELL_0002("The specified function is not recognized"),

  /** An error has occurred when parsing options. */
  SHELL_0003("An error has occurred when parsing options"),

  /** Unable to resolve the variables. */
  SHELL_0004("Unable to resolve the variables"),

  /** We're not able to get user input */
  SHELL_0005("Can't get user input"),

  /** There occurred exception on server side **/
  SHELL_0006("Server has returned exception"),

  /** Job Submission : Cannot sleep */
  SHELL_0007("Cannot sleep"),

  ;

  private final String message;

  private ShellError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
