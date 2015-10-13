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
package org.apache.sqoop.validation.validators;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.CoreError;
import org.apache.sqoop.validation.Status;

/**
 * Validator to test whether an Interger is included in an
 * inclusive interval defined by a string argument with format
 * X,Y
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class InRange extends AbstractValidator<Integer> {
  @Override
  public void validate(Integer instance) {
    if (instance != null) {
      if (getStringArgument().equals(AbstractValidator.DEFAULT_STRING_ARGUMENT))
        throw new SqoopException(CoreError.CORE_0010, "InRange validator needs argument");

      int min;
      int max;
      try {
        min = Integer.parseInt(getStringArgument().split(",")[0]);
        max = Integer.parseInt(getStringArgument().split(",")[1]);
      } catch (NumberFormatException exception) {
        throw new SqoopException(CoreError.CORE_0011, "InRange argument needs to have form MIN,MAX", exception);
      }

      if (instance < min || instance > max) {
        addMessage(Status.ERROR, "Integer must be in interval: [" + getStringArgument() + "]");
      }
    }
  }
}
