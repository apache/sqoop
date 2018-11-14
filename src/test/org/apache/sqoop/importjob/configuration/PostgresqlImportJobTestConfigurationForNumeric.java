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

package org.apache.sqoop.importjob.configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * This test configuration covers the case when postgres returns invalid values
 * for precision and scale for NUMERIC. Also, important, that the accompanying columns
 *  - NUMERIC(20) and NUMERIC(20, 5) don't get modified.
 */
public class PostgresqlImportJobTestConfigurationForNumeric implements ImportJobTestConfiguration, AvroTestConfiguration, ParquetTestConfiguration {

  @Override
  public String[] getTypes() {
    String[] columnTypes = {"INT", "NUMERIC", "NUMERIC(20)", "NUMERIC(20, 5)"};
    return columnTypes;
  }

  @Override
  public String[] getNames() {
    String[] columnNames = {"ID", "N1", "N2", "N3"};
    return columnNames;
  }

  @Override
  public List<String[]> getSampleData() {
    List<String[]> inputData = new ArrayList<>();
    inputData.add(new String[]{"1", "100.01", "100.01", "100.01"});
    return inputData;
  }

  @Override
  public String[] getExpectedResultsForAvro() {
    String expectedRecord = "{\"ID\": 1, \"N1\": 100.010, \"N2\": 100, \"N3\": 100.01000}";
    String[] expectedResult = new String[1];
    expectedResult[0] = expectedRecord;
    return expectedResult;
  }

  @Override
  public String[] getExpectedResultsForParquet() {
    String expectedRecord = "1,100.010,100,100.01000";
    String[] expectedResult = new String[1];
    expectedResult[0] = expectedRecord;
    return expectedResult;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
