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
import org.apache.sqoop.utils.UrlSafeUtils;

/**
 * Represents a <tt>String</tt> input. The boolean flag <tt>sensitive</tt> supplied
 * to its constructor can be used to indicate if the string should be masked
 * from user-view. This is helpful for creating input strings that represent
 * sensitive information such as passwords.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class MStringInput extends MInput<String> {

  private final short maxLength;

  /**
   * @param name the parameter name
   * @param sensitive a flag indicating if the string should be masked
   * @param maxLength the maximum length of the string
   */
  public MStringInput(String name, boolean sensitive, InputEditable editable, String overrides, short maxLength) {
    super(name, sensitive, editable, overrides);
    this.maxLength = maxLength;
  }

  /**
   * @return the maximum length of this string type
   */
  public short getMaxLength() {
    return maxLength;
  }

  @Override
  public String getUrlSafeValueString() {
    return UrlSafeUtils.urlEncode(getValue());
  }

  @Override
  public void restoreFromUrlSafeValueString(String valueString) {
    setValue(UrlSafeUtils.urlDecode(valueString));
  }

  @Override
  public MInputType getType() {
    return MInputType.STRING;
  }

  @Override
  public boolean hasExtraInfo() {
    return true;
  }

  @Override
  public String getExtraInfoToString() {
    return Short.toString(getMaxLength());
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MStringInput)) {
      return false;
    }

    MStringInput msi = (MStringInput) other;
    return getName().equals(msi.getName())
        && (isSensitive() == msi.isSensitive())
        && (maxLength == msi.maxLength);
  }

  @Override
  public int hashCode() {
    int result = 23 + 31 * getName().hashCode();
    result = 31 * result + (isSensitive() ? 1 : 0);
    result = 31 * result + maxLength;
    return result;
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
  public MStringInput clone(boolean cloneWithValue) {
    MStringInput copy = new MStringInput(getName(), isSensitive(), getEditable(), getOverrides(),
        getMaxLength());
    copy.setPersistenceId(getPersistenceId());
    if(cloneWithValue) {
      copy.setValue(this.getValue());
    }
    return copy;
  }
}
