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

package org.apache.sqoop.accumulo;

import java.io.IOException;
import java.util.Map;
import org.apache.accumulo.core.data.Mutation;

/**
 * Abstract class that takes a map of jdbc field names to values
 * and converts them to Mutations for Accumulo.
 */
public abstract class MutationTransformer {

  private String columnFamily;
  private String rowKeyColumn;
  private String visibility;

  /**
   * @return the default column family to insert into.
   */
  public String getColumnFamily() {
    return this.columnFamily;
  }

  /**
   * Set the default column family to insert into.
   */
  public void setColumnFamily(String colFamily) {
    this.columnFamily = colFamily;
  }

  /**
   * @return the field name identifying the value to use as the row id.
   */
  public String getRowKeyColumn() {
    return this.rowKeyColumn;
  }

  /**
   * Set the column of the input fields which should be used to calculate
   * the row id.
   */
  public void setRowKeyColumn(String rowKeyCol) {
    this.rowKeyColumn = rowKeyCol;
  }

  /**
   * @return the field name identifying the visibility token.
   */
  public String getVisibility() {
    return this.visibility;
  }

  /**
   * Set the visibility token to set for each cell.
   */
  public void setVisibility(String vis) {
    this.visibility = vis;
  }

  /**
   * Returns a list of Mutations that inserts the fields into a row in Accumulo.
   * @param fields a map of field names to values to insert.
   * @return A list of Mutations that inserts these into Accumulo.
   */
  public abstract Iterable<Mutation> getMutations(Map<String, Object> fields)
      throws IOException;
}
