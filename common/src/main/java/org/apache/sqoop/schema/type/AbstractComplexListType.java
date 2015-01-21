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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * Complex types that can have nested data as a map or list structure
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class AbstractComplexListType extends AbstractComplexType {

  // represents the type of the list elements
  // NOTE: required for Array/Set, optional for Enum
  Column listType;

  public AbstractComplexListType(String name) {
    super(name);
  }

  public AbstractComplexListType(String name, Column listType) {
    super(name);
    setListType(listType);
  }

  public AbstractComplexListType(String name, Boolean nullable, Column listType) {
    super(name, nullable);
    setListType(listType);
  }

  public AbstractComplexListType setListType(Column listType) {
    assert listType != null;
    this.listType = listType;
    return this;
  }

  public Column getListType() {
    return listType;
  }

  @Override
  public String toString() {
    return new StringBuilder(super.toString()).append(",listType=").append(listType).toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((listType == null) ? 0 : listType.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    AbstractComplexListType other = (AbstractComplexListType) obj;
    if (listType == null) {
      if (other.listType != null)
        return false;
    } else if (!listType.equals(other.listType))
      return false;
    return true;
  }

}
