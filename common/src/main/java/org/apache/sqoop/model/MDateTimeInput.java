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

import org.joda.time.DateTime;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MDateTimeInput extends MInput<DateTime> {

  public MDateTimeInput(String name, boolean sensitive, InputEditable editable, String overrides) {
    super(name, sensitive, editable, overrides);
  }

  @Override
  public String getUrlSafeValueString() {
    if (isEmpty()) {
      return "";
    }

    return String.valueOf(getValue().getMillis());
  }

  @Override
  public void restoreFromUrlSafeValueString(String valueString) {
    if (valueString == null || valueString.isEmpty()) {
      setValue(null);
    } else {
      setValue(new DateTime(Long.parseLong(valueString)));
    }
  }

  @Override
  public MInputType getType() {
    return MInputType.DATETIME;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MDateTimeInput)) {
      return false;
    }

    MDateTimeInput mdi = (MDateTimeInput) other;
    return getName().equals(mdi.getName());
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
  public MDateTimeInput clone(boolean cloneWithValue) {
    MDateTimeInput copy = new MDateTimeInput(getName(), isSensitive(), getEditable(), getOverrides());
    copy.setPersistenceId(getPersistenceId());
    if(cloneWithValue && this.getValue() != null) {
      copy.setValue(new DateTime(this.getValue()));
    }
    return copy;
  }
}
