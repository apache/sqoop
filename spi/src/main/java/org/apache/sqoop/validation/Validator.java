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

import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;

import java.util.List;

/**
 * Connection and job metadata validator.
 *
 * This class should be extended by connector to provide form validation for
 * connection and job forms. All methods are returning only validation Status.
 * List of error and warning messages is directly updated in given structures.
 * You can use methods getErrorMessage and getWarningMessage to retrieve them
 * or setErrorMessage and setWarningMessage to set them.
 *
 * There are two major use cases of this class - simple and advanced. In simple
 * mode connector developer should override method validate(MForm). This method
 * will get exactly one form for validation at the time. All other methods in
 * default implementation will eventually delegate call to this method. There
 * is no implicit way how to distinguish between connection and job form or how
 * to reference different forms.
 *
 * In advance usage user should override methods validate(MConnectionForms) and
 * validate(MJobForms). Both methods will be called with all form gathered so far
 * and connector developer might use information from all forms to do cross
 * form validations.
 */
public class Validator {

  /**
   * Validate one single form.
   *
   * @param form Form to be validated
   * @return Validation status
   */
  public Status validate(MForm form) {
    return Status.FINE;
  }

  /**
   * Internal method used to validate arbitrary list of forms.
   *
   * It's delegating validation to validate(MForm) method. Return
   * status will be the highest defined in Status enumeration (e.g. the worst).
   *
   * @param forms List of forms to be validated
   * @return Validation status
   */
  protected Status validate(List<MForm> forms) {
    Status finalStatus = Status.FINE;
    for (MForm form : forms) {
      Status status = validate(form);

      if ( finalStatus.compareTo(status) > 0 ) {
        finalStatus = status;
      }
    }

    return finalStatus;
  }

  /**
   * Validate connection forms.
   *
   * This method will be called when user will try to create new connection
   * in the system. It must return FINE or ACCEPTABLE in order to proceed and
   * save the job in metadata repository.
   *
   * Default implementation will delegate the task to validate(MForm).
   *
   * @param connection Connection to be validated
   * @return Validation status
   */
  public Status validate(MConnectionForms connection) {
    return validate(connection.getForms());
  }

  /**
   * Validate job forms.
   *
   * This method will be called when user will try to create new job in the
   * system. It must return FINE or ACCEPTABLE in order to proceed and save
   * the job in metadata repository.
   *
   * Default implementation will delegate the job to validate(MForm).
   *
   * @param job Job to be validated
   * @return Validation status
   */
  public Status validate(MJobForms job) {
    return validate(job.getForms());
  }

}
