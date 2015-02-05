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
 * Represents a parameter input used by the connector for creating a link
 * or a job object.
 * @param <T> the value type associated with this parameter
 * @param boolean whether or not the field contains sensitive information
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class MInput<T> extends MValidatedElement implements MClonable {
  private final boolean sensitive;

  private final String overrides;

  private final InputEditable editable;

  private T value;

  protected MInput(String name, boolean sensitive, InputEditable editable, String overrides) {
    super(name);
    this.sensitive = sensitive;
    this.editable = editable;
    this.overrides = overrides;
  }

  /**
   * @param value
   *          the value to be set for this parameter
   */
  public void setValue(T value) {
    this.value = value;
  }

  /**
   * @return any previously set value for this parameter
   */
  public T getValue() {
    return value;
  }

  /**
   * @return <tt>true</tt> if this string represents sensitive information
   */
  public boolean isSensitive() {
    return sensitive;
  }

  /**
   * @return the editable {@link#InputEditable}attribute for the input
   */
  public InputEditable getEditable() {
    return editable;
  }

  /**
   * @return the overrides attribute for the input
   * An input can override the value of one or more other inputs when edited
   */
  public String getOverrides() {
    return overrides;
  }

  /**
  /**
   * @return a URL-safe string representation of the value
   */
  public abstract String getUrlSafeValueString();

  /**
   * Overrides the associated value of this input by the value represented by
   * the provided URL-safe value string.
   * @param valueString the string representation of the value from which the
   * value must be restored.
   */
  public abstract void restoreFromUrlSafeValueString(String valueString);

  public abstract MInputType getType();

  /**
   * @return <tt>true</tt> if this type maintains more state than what is
   * stored in the <tt>MInput</tt> base class.
   */
  public boolean hasExtraInfo() {
    return false;
  }

  /**
   * @return the string representation of state stored in this type if
   * applicable or an empty string.
   */
  public String getExtraInfoToString() {
    return null;
  }

  /**
   * All input types must override the <tt>equals()</tt> method such that the
   * test for equality is based on static metadata only. As a result any
   * set value, error message and other dynamic value data is not considered
   * as part of the equality comparison.
   */
  @Override
  public abstract boolean equals(Object other);

  /**
   * All input types must override the <tt>hashCode()</tt> method such that
   * the hash code computation is solely based on static metadata. As a result
   * any set value, error message and other dynamic value data is not
   * considered as part of the hash code computation.
   */
  @Override
  public abstract int hashCode();

  /**
   * All input types must be able to tell if they contain some value or not.
   *
   * Empty values won't be serialized into metadata repository and will not be
   * send across the wire between client and server.
   *
   * @return True if this input contains empty value.
   */
  public abstract boolean isEmpty();

  /**
   * Set Input value to empty value.
   */
  public abstract void setEmpty();

  @Override
  public final String toString() {
    StringBuilder sb = new StringBuilder("input-").append(getName());
    sb.append(":").append(getPersistenceId()).append(":");
    sb.append(getType()).append(":").append(isSensitive()).append(":").append(getEditable().name())
        .append(":").append(getOverrides());
    if (hasExtraInfo()) {
      sb.append(":").append(getExtraInfoToString());
    }

    return sb.toString();
  }
}
