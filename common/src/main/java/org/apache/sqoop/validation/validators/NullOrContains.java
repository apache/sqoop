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
import org.apache.sqoop.validation.Status;

/**
 * String validator to verify presence of a sub string (provided as a String argument)
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class NullOrContains extends AbstractValidator<String> {
  @Override
  public void validate(String str) {
    if(str != null && !str.contains(getStringArgument())) {
      addMessage(Status.ERROR, "String must contain substring: " + getStringArgument());
    }
  }
}
