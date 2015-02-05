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
 * Integer base user input.
 *
 * This input is able to process empty (NULL) value.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MIntegerInput extends MInput<Integer> {

  public MIntegerInput(String name, boolean sensitive, InputEditable editable, String overrides) {
    super(name, sensitive, editable, overrides);
  }

  @Override
  public String getUrlSafeValueString() {
    if(isEmpty()) {
      return "";
    }

    return getValue().toString();
  }

  @Override
  public void restoreFromUrlSafeValueString(String valueString) {
    if(valueString.isEmpty()) {
      setEmpty();
    }

    setValue(Integer.valueOf(valueString));
  }

  @Override
  public MInputType getType() {
    return MInputType.INTEGER;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MIntegerInput)) {
      return false;
    }

    MIntegerInput i = (MIntegerInput) other;
    return getName().equals(i.getName());
  }

  @Override
  public int hashCode() {
    return 23 + 31 * getName().hashCode();
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
  public MIntegerInput clone(boolean cloneWithValue) {
    MIntegerInput copy = new MIntegerInput(getName(), isSensitive(), getEditable(), getOverrides());
    copy.setPersistenceId(getPersistenceId());
    if(cloneWithValue) {
      copy.setValue(this.getValue());
    }
    return copy;
  }
}
