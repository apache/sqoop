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

import org.apache.sqoop.validation.Status;

/**
 * Element that can be validated for correctness.
 *
 * Two severity levels are supported at the moment - warning and error.
 *
 * Warning:
 * Warning is something suspicious, potentially wrong but something that
 * can be ignored. For example in case of JDBC URL element, warning would
 * be if specified host is not responding - it's warning because specified
 * URL might be wrong. However at the same time URL might be right as only
 * target host might be down.
 *
 * Error:
 * Error represents unacceptable element content. For example in case of JDBC
 * URL path, error would be empty element or element containing invalid URL.
 */
public abstract class MValidatedElement extends MNamedElement {

  /**
   * Validation message.
   *
   * One element can have only one message regardless of the type.
   */
  private String validationMessage;

  /**
   * Severity of the message.
   */
  private Status validationStatus;

  public MValidatedElement(String name) {
    super(name);
    // Everything is fine by default
    this.validationStatus = Status.getDefault();
  }

  /**
   * Set validation message and given severity.
   *
   * @param status Message validation status
   * @param msg Message itself
   */
  public void setValidationMessage(Status status, String msg) {
    this.validationMessage = msg;
    this.validationStatus = status;
  }

  /**
   * Return validation message for given severity.
   *
   * Return either associated message for given severity or null in case
   * that there is no message with given severity.
   *
   * @param status Message validation status
   */
  public String getValidationMessage(Status status) {
    return (validationStatus.equals(status)) ? validationMessage : null;
  }

  /**
   * Return validation message.
   *
   * Return current validation message.
   */
  public String getValidationMessage() {
    return validationMessage;
  }

  /**
   * Return message validation status.
   */
  public Status getValidationStatus() {
    return validationStatus;
  }

  /**
   * Set error message for this element.
   *
   * @param errMsg Error message
   */
  public void setErrorMessage(String errMsg) {
    setValidationMessage(Status.UNACCEPTABLE, errMsg);
  }

  /**
   * Return error message associated with this element.
   *
   * @return Error message
   */
  public String getErrorMessage() {
    return getValidationMessage(Status.UNACCEPTABLE);
  }

  /**
   * Set warning message for this element.
   *
   * @param warnMsg Warning message
   */
  public void setWarningMessage(String warnMsg) {
    setValidationMessage(Status.ACCEPTABLE, warnMsg);
  }

  /**
   * Retrieve warning message associated with this element.
   * @return
   */
  public String getWarningMessage() {
    return getValidationMessage(Status.ACCEPTABLE);
  }

}
