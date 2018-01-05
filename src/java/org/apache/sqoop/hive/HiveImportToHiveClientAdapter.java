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

package org.apache.sqoop.hive;

import java.io.IOException;

public class HiveImportToHiveClientAdapter implements HiveClient {

  private final HiveImport hiveImport;

  private final String inputTableName;

  private final String outputTableName;

  public HiveImportToHiveClientAdapter(HiveImport hiveImport, String inputTableName, String outputTableName) {
    this.hiveImport = hiveImport;
    this.inputTableName = inputTableName;
    this.outputTableName = outputTableName;
  }

  @Override
  public void importTable() throws IOException {
    hiveImport.importTable(inputTableName, outputTableName, false);
  }

  @Override
  public void createTable() throws IOException {
    hiveImport.importTable(inputTableName, outputTableName, true);
  }
}
