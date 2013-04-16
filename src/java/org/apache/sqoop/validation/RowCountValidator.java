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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A specific implementation of validator that validates data copied,
 * either import or export, using row counts from the data source and
 * the target systems.
 *
 * This is used as the default validator unless overridden in configuration.
 */
public class RowCountValidator implements Validator {

  public static final Log LOG = LogFactory.getLog(
    RowCountValidator.class.getName());

  @Override
  public boolean validate(ValidationContext context)
    throws ValidationException {
    return validate(context,
      AbsoluteValidationThreshold.INSTANCE, AbortOnFailureHandler.INSTANCE);
  }

  @Override
  public boolean validate(ValidationContext validationContext,
                          ValidationThreshold validationThreshold,
                          ValidationFailureHandler validationFailureHandler)
  throws ValidationException {
    LOG.debug("Validating data using row counts: Source ["
      + validationContext.getSourceRowCount() + "] with Target["
      + validationContext.getTargetRowCount() + "]");

    if (validationThreshold.compare(validationContext.getSourceRowCount(),
      validationContext.getTargetRowCount())) {
      LOG.info("Data successfully validated");
      return true;
    }

    validationContext.setMessage(this.getClass().getSimpleName());
    validationContext.setReason("The expected counter value was "
      + validationContext.getSourceRowCount() + " but the actual value was "
      + validationContext.getTargetRowCount());

    return validationFailureHandler.handle(validationContext);
  }
}
