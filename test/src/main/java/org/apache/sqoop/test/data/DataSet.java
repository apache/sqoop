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
package org.apache.sqoop.test.data;

import org.apache.sqoop.common.test.db.DatabaseProvider;
import org.apache.sqoop.common.test.db.TableName;

/**
 * Abstract class for basic testing data sets.
 *
 * Each data set provides couple of generic methods that can be used to set up
 * the tables and load example data.
 */
public abstract class DataSet {

  /**
   * Database provider that will be used to populate the data.
   */
  protected DatabaseProvider provider;

  /**
   * Base name for created tables.
   */
  protected TableName tableBaseName;

  public DataSet(DatabaseProvider provider, TableName tableBaseName) {
    setProvider(provider);
    setTableBaseName(tableBaseName);
  }

  public DataSet setProvider(DatabaseProvider provider) {
    this.provider = provider;
    return this;
  }

  public DataSet setTableBaseName(TableName tableBaseName) {
    this.tableBaseName = tableBaseName;
    return this;
  }

  /**
   * Crate all tables that this testing data set might need.
   */
  public abstract DataSet createTables();

  /**
   * Load basic data.
   *
   * Basic data set should be small (around 10 rows) without any specialities.
   */
  public abstract DataSet loadBasicData();
}
