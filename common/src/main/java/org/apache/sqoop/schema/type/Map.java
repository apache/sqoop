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
 * Associative array.
 *
 * JDBC Types: map
 */
public class Map extends AbstractComplexType {

  // They key can be either a string or number
  private AbstractPrimitiveType key;
  // The value inside the map can be either a primitive or a complex column type
  private Column value;

  public Map(AbstractPrimitiveType key, Column value) {
    super();
    setKeyValue(key, value);
  }

  public Map(String name, AbstractPrimitiveType key, Column value) {
    super(name);
    setKeyValue(key, value);
  }

  public Map(String name, Boolean nullable, AbstractPrimitiveType key, Column value) {
    super(name, nullable);
    setKeyValue(key, value);
  }

  private void setKeyValue(AbstractPrimitiveType key, Column value) {
    assert key != null;
    assert value != null;
    this.key = key;
    this.value = value;
  }

  @Override
  public ColumnType getType() {
    return ColumnType.MAP;
  }

  public AbstractPrimitiveType getKey() {
    return key;
  }

  public Column getValue() {
    return value;
  }

  @Override
  public String toString() {
    return new StringBuilder("Map{").append(super.toString()).append(",key=").append(key)
        .append(",value=").append(value).append("}").toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Map))
      return false;
    if (!super.equals(o))
      return false;

    Map map = (Map) o;

    if (key != null ? !key.equals(map.key) : map.key != null)
      return false;

    if (value != null ? !value.equals(map.value) : map.value != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (key != null ? key.hashCode() : 0);
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }
}
