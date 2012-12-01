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

/**
 * This object encapsulates the context for the validation framework.
 * Before validation, the row counts are stored. Post validation,
 * the message and failure reason are captured.
 */
public class ValidationContext {
  private final long sourceRowCount;
  private final long targetRowCount;

  private String message;
  private String reason;

  public ValidationContext(long sourceRowCount, long targetRowCount) {
    this.sourceRowCount = sourceRowCount;
    this.targetRowCount = targetRowCount;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String aMessage) {
    this.message = aMessage;
  }

  public String getReason() {
    return reason;
  }

  public void setReason(String aReason) {
    this.reason = aReason;
  }

  public long getSourceRowCount() {
    return sourceRowCount;
  }

  public long getTargetRowCount() {
    return targetRowCount;
  }
}
