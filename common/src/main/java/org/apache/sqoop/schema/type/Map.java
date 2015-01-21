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
 * Associative array.
 *
 * JDBC Types: map
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Map extends AbstractComplexType {

  // They key can be either a string or number
  private AbstractPrimitiveType key;
  // The value inside the map can be either a primitive or a complex column type
  private Column value;

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
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
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
    Map other = (Map) obj;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    return true;
  }

}
