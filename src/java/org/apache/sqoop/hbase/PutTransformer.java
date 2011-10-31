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

package org.apache.sqoop.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;

/**
 * Interface that takes a map of jdbc field names to values
 * and converts them to a Put command for HBase.
 */
public abstract class PutTransformer {

  private String columnFamily;
  private String rowKeyColumn;

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
   * Returns a list of Put commands that inserts the fields into a row in HBase.
   * @param fields a map of field names to values to insert.
   * @return A list of Put commands that inserts these into HBase.
   */
  public abstract List<Put> getPutCommand(Map<String, Object> fields)
      throws IOException;

}
