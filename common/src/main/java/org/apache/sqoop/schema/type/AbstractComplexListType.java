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
 * Complex types that can have nested data as a map or list structure
 */
public abstract class AbstractComplexListType extends AbstractComplexType {

  // represents the type of the list elements
  Column listType;

  public AbstractComplexListType(Column listType) {
    super();
    setListType(listType);
  }

  public AbstractComplexListType(String name, Column listType) {
    super(name);
    setListType(listType);
  }

  public AbstractComplexListType(String name, Boolean nullable, Column listType) {
    super(name, nullable);
    setListType(listType);
  }

  private void setListType(Column listType) {
    assert listType != null;
    this.listType = listType;
  }

  public Column getListType() {
    return listType;
  }

  @Override
  public String toString() {
    return new StringBuilder(super.toString()).append(",listType=").append(listType).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof AbstractComplexListType))
      return false;
    if (!super.equals(o))
      return false;

    AbstractComplexListType that = (AbstractComplexListType) o;

    if (listType != null ? !listType.equals(that.listType) : that.listType != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (listType != null ? listType.hashCode() : 0);
    return result;
  }

}
