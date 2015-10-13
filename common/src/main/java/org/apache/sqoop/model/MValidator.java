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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * Represents an @Validator class by its name and optional argument
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MValidator implements MClonable {
  private final String validatorClass;
  private final String strArg;

  public MValidator(String validatorClass, String strArg) {
    this.validatorClass = validatorClass;
    this.strArg = strArg;
  }

  // The value of cloneWithValue is ignored
  @Override
  public Object clone(boolean cloneWithValue) {
    return new MValidator(validatorClass, strArg);
  }

  public String getValidatorClass() {
    return validatorClass;
  }

  public String getStrArg() {
    return strArg;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MValidator)) return false;

    MValidator that = (MValidator) o;

    if (!getValidatorClass().equals(that.getValidatorClass())) return false;
    return getStrArg().equals(that.getStrArg());
  }

  @Override
  public int hashCode() {
    int result = getValidatorClass().hashCode();
    result = 31 * result + getStrArg().hashCode();
    return result;
  }
}
