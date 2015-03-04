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
package org.apache.sqoop.common.test.db.types;

import java.util.LinkedList;
import java.util.List;

/**
 * Class describing type and example values that can be stored in the database.
 */
public class DatabaseType {

  /**
   * Builder for simpler creation of the DatabaseType objects
   */
  public static class Builder {
    private String name;
    List<ExampleValue> values;

    public Builder(String name) {
      this.name = name;
      values = new LinkedList<ExampleValue>();
    }

    public Builder addExample(String insertStatement, Object objectValue, String escapedStringValue) {
      values.add(new ExampleValue(insertStatement, objectValue, escapedStringValue));
      return this;
    }

    public DatabaseType build() {
      return new DatabaseType(name, values);
    }
  }

  /**
   * Return new instance of builder.
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  /**
   * Name of type as can appear in the CREATE TABLE statement.
   */
  public final String name;

  /**
   * Example values for given data type.
   *
   * Small number of the values, not exhaustive list.
   */
  public final List<ExampleValue> values;

  public DatabaseType(String name, List<ExampleValue> values) {
    this.name = name;
    this.values = values;
  }

  @Override
  public String toString() {
    return name;
  }

  /**
   * Returns escaped strings from all values in it's own array.
   *
   * The order is defined and will always be in the same order as
   * is present in the values array.
   *
   * @return Array where each item represents escapeStringValue field from the
   *   corresponding values element
   */
  public String []escapedStringValues() {
    String [] ret = new String[values.size()];
    int i = 0;
    for(ExampleValue value : values) {
      ret[i++] = value.escapedStringValue;
    }
    return ret;
  }
}
