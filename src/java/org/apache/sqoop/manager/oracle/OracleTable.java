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

package org.apache.sqoop.manager.oracle;

/**
 * Contains details about an Oracle table.
 */
public class OracleTable {

  private String schema;
  private String name;

  public String getSchema() {
    return schema;
  }

  private void setSchema(String newSchema) {
    this.schema = newSchema;
  }

  public String getName() {
    return name;
  }

  private void setName(String newName) {
    this.name = newName;
  }

  public OracleTable() {

  }

  public OracleTable(String schema, String name) {

    setSchema(schema);
    setName(name);
  }

  public OracleTable(String name) {
    setName(name);
  }

  @Override
  public String toString() {
    String result =
        (getSchema() == null || getSchema().isEmpty()) ? "" : "\""
            + getSchema() + "\".";
    result += "\"" + getName() + "\"";
    return result;
  }

}
