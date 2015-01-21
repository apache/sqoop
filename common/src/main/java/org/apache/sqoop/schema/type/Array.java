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
 * Array contains multiple values of the same type.
 *
 * JDBC Types: array
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Array extends AbstractComplexListType {

  /**
   * Represents the size for the column type and will be handy for connectors to
   * map this info to the native data sources they represent
   * https://issues.apache.org/jira/secure/attachment/12589331/Sqoop2Datatypes.pdf
   * NOTE : only certain data sources such as Postgres support size attribute for arrays
   */
  private Long size;

  public Array(String name, Column listType) {
    super(name, listType);
  }

  public Array(String name, Boolean nullable, Column listType) {
    super(name, nullable, listType);
  }

  public Long getSize() {
    return size;
  }

  public Array setSize(Long size) {
    this.size = size;
    return this;
  }

  @Override
  public ColumnType getType() {
    return ColumnType.ARRAY;
  }

  @Override
  public String toString() {
    return new StringBuilder("Array{").
         append(super.toString()).
         append(", size=").
         append(size).
         append("}").toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Array))
      return false;
    if (!super.equals(o))
      return false;

    Array that = (Array) o;

    if (size != null ? !size.equals(that.size) : that.size != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (size != null ? size.hashCode() : 0);
    return result;
  }

}
