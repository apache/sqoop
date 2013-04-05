/**
 * Copyright 2011 The Apache Software Foundation
 *
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

package org.apache.sqoop.validation;

/**
 * A specific implementation of ValidationFailureHandler that aborts the
 * processing by throwing an exception with failure message and the reason.
 *
 * This is used as the default handler unless overridden in configuration.
 */
public class AbortOnFailureHandler implements ValidationFailureHandler {

  static final ValidationFailureHandler INSTANCE = new AbortOnFailureHandler();

  /**
   * Method that handles the validation failure.
   *
   * @param validationContext validation context
   * @return if failure was handled or not
   * @throws ValidationException
   */
  @Override
  public boolean handle(ValidationContext validationContext)
    throws ValidationException {

    StringBuilder messageBuffer = new StringBuilder();
    messageBuffer.append("Validation failed by ");
    messageBuffer.append(validationContext.getMessage());
    messageBuffer.append(". Reason: ").append(validationContext.getReason());
    messageBuffer.append(", Row Count at Source: ");
    messageBuffer.append(validationContext.getSourceRowCount());
    messageBuffer.append(", Row Count at Target: ");
    messageBuffer.append(validationContext.getTargetRowCount());

    throw new ValidationException(messageBuffer.toString());
  }
}
