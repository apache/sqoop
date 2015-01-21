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

import java.util.Set;
import java.util.HashSet;

/**
 * Enum is a set of predefined values of its own type
 *
 * JDBC Types: enum
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Enum extends Column {

  // The options set contains the possible values for the Enum
  private Set<String> options;

  public Enum(String name) {
    super(name);
    setOptions(new HashSet<String>());
  }

  public Enum(String name, Set<String> options) {
    super(name);
    setOptions(options);
  }

  public Enum setOptions(Set<String> options) {
    assert options != null;
    this.options = options;
    return this;
  }

  public Set<String> getOptions() {
    return options;
  }

  @Override
  public ColumnType getType() {
    return ColumnType.ENUM;
  }

  @Override
  public String toString() {
    return new StringBuilder("Enum{").append(super.toString()).append(",options=").append(options)
        .append("}").toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Enum)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    Enum that = (Enum) o;

    if (options != null ? !options.equals(that.options) : that.options != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (options != null ? options.hashCode() : 0);
    return result;
  }
}
