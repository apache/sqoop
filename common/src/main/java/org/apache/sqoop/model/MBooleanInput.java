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
 * Represents a <tt>Boolean</tt> input.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MBooleanInput extends MInput<Boolean> {

  public MBooleanInput(String name, boolean sensitive, InputEditable editable, String overrides) {
    super(name, sensitive, editable, overrides);
  }

  @Override
  public String getUrlSafeValueString() {
    return getValue() ? "true" : "false";
  }

  @Override
  public void restoreFromUrlSafeValueString(String valueString) {
    if("true".equals(valueString)) {
      setValue(true);
    } else {
      setValue(false);
    }
  }

  @Override
  public MInputType getType() {
    return MInputType.BOOLEAN;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof MBooleanInput)) {
      return false;
    }

    MBooleanInput mbi = (MBooleanInput)other;
    return getName().equals(mbi.getName())
      && (isSensitive() == mbi.isSensitive());
  }

  @Override
  public int hashCode() {
    return getValue().hashCode();
  }

  @Override
  public boolean isEmpty() {
    return getValue() == null;
  }

  @Override
  public void setEmpty() {
    setValue(null);
  }

  @Override
  public Object clone(boolean cloneWithValue) {
    MBooleanInput copy = new MBooleanInput(getName(), isSensitive(), getEditable(), getOverrides());
    copy.setPersistenceId(getPersistenceId());
    if(cloneWithValue) {
      copy.setValue(getValue());
    }
    return copy;
  }
}
