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
 * Contains details about a column in an Oracle table.
 */
public class OracleTableColumn {

  private String name;
  private String dataType; // <- i.e. The data_type from dba_tab_columns
  private int oracleType;

  public OracleTableColumn(String name, String dataType) {

    this.setName(name);
    this.setDataType(dataType);
  }

  public String getName() {
    return name;
  }

  public void setName(String newName) {
    this.name = newName;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String newDataType) {
    this.dataType = newDataType;
  }

  public int getOracleType() {
    return oracleType;
  }

  public void setOracleType(int newOracleType) {
    this.oracleType = newOracleType;
  }
}
