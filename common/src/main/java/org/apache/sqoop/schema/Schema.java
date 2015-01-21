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
package org.apache.sqoop.schema;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.schema.type.Column;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema represents the data fields that are transferred between {@link #From}
 * and {@link #To}
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Schema {

  /**
   * Name of the schema, usually a table name.
   */
  private String name;

  /**
   * Optional note.
   */
  private String note;

  /**
   * Creation date.
   */
  private Date creationDate;

  /**
   * Columns associated with the schema.
   */
  private List<Column> columns;

  /**
   * Helper set for quick column name lookups.
   */
  private Set<String> columNames;

  /**
   * Helper map to map column name to index
   */
  private Map<String, Integer> nameToIndexMap;

  /**
   * column global index
   */

  int columnIndex;

  private Schema() {
    creationDate = new Date();
    columns = new ArrayList<Column>();
    columNames = new HashSet<String>();
    nameToIndexMap = new HashMap<String, Integer>();
  }

  public Schema(String name) {
    this();
    if (StringUtils.isEmpty(name)) {
      throw new SqoopException(SchemaError.SCHEMA_0006, "Schema: " + name);
    }
    this.name = name;
  }

  /**
   * Add column to the schema.
   *
   * Add new column to the schema at the end (e.g. after all previously added
   * columns). The column names must be unique and thus adding column with the
   * same name will lead to an exception being thrown.
   *
   * @param column Column that should be added to the schema at the end.
   * @return a reference to this object
   */
  public Schema addColumn(Column column) {
    if(columNames.contains(column.getName())) {
      throw new SqoopException(SchemaError.SCHEMA_0002, "Column: " + column);
    }
    columNames.add(column.getName());
    columns.add(column);
    nameToIndexMap.put(column.getName(), columnIndex);
    columnIndex ++;
    return this;
  }

  public String getName() {
    return name;
  }

  public Date getCreationDate() {
    return creationDate;
  }

  public String getNote() {
    return note;
  }

  public Schema setNote(String note) {
    this.note = note;
    return this;
  }

  public Schema setCreationDate(Date creationDate) {
    this.creationDate = creationDate;
    return this;
  }

  public Column[] getColumnsArray() {
    return columns.toArray(new Column[columns.size()]);
  }

  public List<Column> getColumnsList() {
    return columns;
  }

  public int getColumnsCount() {
    return columns.size();
  }

  public Integer getColumnNameIndex(String name) {
    if (columNames.contains(name)) {
      return nameToIndexMap.get(name);
    }
    throw new SqoopException(SchemaError.SCHEMA_0007, "Column: " + name);
  }

  public boolean isEmpty() {
    return columns.size() == 0;
  }

  public String toString() {
    return new StringBuilder("Schema{")
      .append("name=").append(name).append("")
      .append(",columns=[\n\t").append(StringUtils.join(columns, ",\n\t")).append("]")
      .append("}")
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Schema)) return false;

    Schema schema = (Schema) o;

    if (columns != null ? !columns.equals(schema.columns) : schema.columns != null)
      return false;
    if (name != null ? !name.equals(schema.name) : schema.name != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (columns != null ? columns.hashCode() : 0);
    return result;
  }
}
