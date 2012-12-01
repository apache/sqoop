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
 * This represents the primary interface that drives the validation logic
 * by delegating the decision to ValidationThreshold and failure handling
 * to ValidationFailureHandler. Uses ValidationContext to encapsulate
 * the various required parameters.
 */
public interface Validator {

  /**
   * Method to validate the data copy with default implementations
   * for ValidationThreshold and ValidationFailureHandler.
   *
   * @param validationContext validation context
   * @return if validation was successful or not
   * @throws ValidationException
   */
  boolean validate(ValidationContext validationContext)
    throws ValidationException;

  /**
   * Method to validate the data copy with specific implementations
   * for ValidationThreshold and ValidationFailureHandler.
   *
   * @param validationContext validation context
   * @param validationThreshold specific implementation of ValidationThreshold
   * @param validationFailureHandler specific implementation of
   *                                 ValidationFailureHandler
   * @return if validation was successful or not
   * @throws ValidationException
   */
  boolean validate(ValidationContext validationContext,
                   ValidationThreshold validationThreshold,
                   ValidationFailureHandler validationFailureHandler)
    throws ValidationException;
}
