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
package org.apache.sqoop.schema.type;

/**
 * Complex types that are incorporating primitive types.
 */
public abstract  class AbstractComplexType extends Column {

  /**
   * Incorporated type
   */
  private Column key;

  public AbstractComplexType(Column key) {
    setKey(key);
  }

  public AbstractComplexType(String name, Column key) {
    super(name);
    setKey(key);
  }

  public AbstractComplexType(String name, Boolean nullable, Column key) {
    super(name, nullable);
    setKey(key);
  }

  public Column getKey() {
    return key;
  }

  public void setKey(Column key) {
    assert key != null;

    this.key = key;
  }

  @Override
  public String toString() {
    return new StringBuilder(super.toString())
      .append(",key=").append(key.toString())
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AbstractComplexType)) return false;
    if (!super.equals(o)) return false;

    AbstractComplexType that = (AbstractComplexType) o;

    if (key != null ? !key.equals(that.key) : that.key != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (key != null ? key.hashCode() : 0);
    return result;
  }
}
