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
 * Parent of all Sqoop types.
 */
public abstract class Column {

  /**
   * Name of the column.
   */
  String name;

  /**
   * Whether NULL is allowed or not.
   */
  Boolean nullable;

  public Column() {
  }

  public Column(String name) {
    setName(name);
  }

  public Column(String name, Boolean nullable) {
    setName(name);
    setNullable(nullable);
  }

  /**
   * Return type of the Column.
   *
   * @return Type of the column
   */
  public abstract Type getType();

  public Column setName(String name) {
    this.name = name;
    return this;
  }

  public Column setNullable(Boolean nullable) {
    this.nullable = nullable;
    return this;
  }

  public Boolean getNullable() {
    return nullable;
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return new StringBuilder()
      .append("name=").append(name).append(",")
      .append("nullable=").append(nullable)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Column)) return false;

    Column that = (Column) o;

    if (name != null ? !name.equals(that.name) : that.name != null)
      return false;
    if (nullable != null ? !nullable.equals(that.nullable) : that.nullable != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (nullable != null ? nullable.hashCode() : 0);
    return result;
  }
}
