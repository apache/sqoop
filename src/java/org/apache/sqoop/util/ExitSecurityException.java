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

package org.apache.sqoop.util;

/**
 * SecurityException suppressing a System.exit() call.
 *
 * Allows retrieval of the would-be exit status code.
 */
@SuppressWarnings("serial")
public class ExitSecurityException extends SecurityException {

  private final int exitStatus;

  public ExitSecurityException() {
    super("ExitSecurityException");
    this.exitStatus = 0;
  }

  public ExitSecurityException(final String message) {
    super(message);
    this.exitStatus = 0;
  }

  /**
   * Register a System.exit() event being suppressed with a particular
   * exit status code.
   */
  public ExitSecurityException(int status) {
    super("ExitSecurityException");
    this.exitStatus = status;
  }

  @Override
  public String toString() {
    String msg = getMessage();
    return (null == msg) ? ("exit with status " + exitStatus) : msg;
  }

  public int getExitStatus() {
    return this.exitStatus;
  }

}
