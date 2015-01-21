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
package org.apache.sqoop.common;


import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * Base exception for Sqoop driver. This exception requires the specification
 * of an error code for reference purposes. Where necessary the appropriate
 * constructor can be used to pass in additional message beyond what is
 * specified by the error code and/or the causal exception.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@SuppressWarnings("serial")
public class SqoopException extends RuntimeException {

  private final ErrorCode code;
  private final String originalMessage;

  public SqoopException(ErrorCode code) {
    super(code.getCode() + ":" + code.getMessage());
    this.code = code;
    originalMessage = null;
  }

  public SqoopException(ErrorCode code, String extraInfo) {
    super(code.getCode() + ":" + code.getMessage() + " - " + extraInfo);
    this.code = code;
    originalMessage = extraInfo;
  }

  public SqoopException(ErrorCode code, Throwable cause) {
    super(code.getCode() + ":" + code.getMessage(), cause);
    this.code = code;
    originalMessage = null;
  }

  public SqoopException(ErrorCode code, String extraInfo, Throwable cause) {
    super(code.getCode() + ":" + code.getMessage() + " - " + extraInfo, cause);
    this.code = code;
    originalMessage = extraInfo;
  }

  public ErrorCode getErrorCode() {
    return code;
  }

  public String getOriginalMessage() {
    return originalMessage;
  }
}
