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

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;

import java.util.Arrays;

/**
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MEnumInput extends MInput<String> {

  /**
   * Array of available values
   */
  String []values;

  public MEnumInput(String name, boolean sensitive, InputEditable editable, String overrides, String[] values) {
    super(name, sensitive, editable, overrides);
    this.values = values;
  }

  public String[] getValues() {
    return values;
  }

  @Override
  public void setValue(String value) {
    // Null is allowed value
    if(value == null) {
      super.setValue(null);
      return;
    }

    // However non null values must be available from given enumeration list
    for(String allowedValue : values) {
      if(allowedValue.equals(value)) {
        super.setValue(value);
        return;
      }
    }

    // Otherwise we've got invalid value
    throw new SqoopException(ModelError.MODEL_008,
      "Invalid value " + value);
  }

  public void setValue(Enum value) {
    setValue(value.toString());
  }

  @Override
  public String getUrlSafeValueString() {
    return getValue();
  }

  @Override
  public void restoreFromUrlSafeValueString(String valueString) {
    setValue(valueString);
  }

  @Override
  public MInputType getType() {
    return MInputType.ENUM;
  }

  @Override
  public boolean hasExtraInfo() {
    return true;
  }

  @Override
  public String getExtraInfoToString() {
    return StringUtils.join(values, ",");
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MEnumInput)) {
      return false;
    }

    MEnumInput mei = (MEnumInput) other;
    return getName().equals(mei.getName()) && Arrays.equals(values, mei.values);
  }

  @Override
  public int hashCode() {
    int hash = 23 + 31 * getName().hashCode();
    for(String value : values) {
      hash += 31 * value.hashCode();
    }

    return hash;
  }

  @Override
  public boolean isEmpty() {
    return getValue() == null;
  }

  @Override
  public void setEmpty() {
    setValue((String)null);
  }

  @Override
  public MEnumInput clone(boolean cloneWithValue) {
    MEnumInput copy = new MEnumInput(getName(), isSensitive(), getEditable(), getOverrides(),
        getValues());
    copy.setPersistenceId(getPersistenceId());
    if(cloneWithValue) {
      copy.setValue(this.getValue());
    }
    return copy;
  }
}
