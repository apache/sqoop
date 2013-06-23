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

import org.apache.sqoop.test.db.DatabaseProvider;

/**
 * Simple listing of few world's cities to do basic sanity tests.
 */
public class Cities extends DataSet {

  public Cities(DatabaseProvider provider, String tableBaseName) {
    super(provider, tableBaseName);
  }

  @Override
  public DataSet createTables() {
    provider.createTable(
      tableBaseName,
      "id",
      "id", "int",
      "country", "varchar(50)",
      "city", "varchar(50)"
    );

    return this;
  }

  @Override
  public DataSet loadBasicData() {
    provider.insertRow(tableBaseName, 1, "USA", "San Francisco");
    provider.insertRow(tableBaseName, 2, "USA", "Sunnyvale");
    provider.insertRow(tableBaseName, 3, "Czech Republic", "Brno");
    provider.insertRow(tableBaseName, 4, "USA", "Palo Alto");

    return this;
  }
}
