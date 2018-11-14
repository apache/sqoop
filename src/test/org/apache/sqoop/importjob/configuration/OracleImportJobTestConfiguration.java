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
 * This test configuration intends to cover the fact that oracle stores these types without padding them with 0s,
 * therefore when importing into avro, one has to use the padding feature.
 */
public class OracleImportJobTestConfiguration implements ImportJobTestConfiguration, AvroTestConfiguration, ParquetTestConfiguration {

  @Override
  public String[] getTypes() {
    return new String[]{"INT", "NUMBER(20)", "NUMBER(20,5)", "NUMBER(20,-5)", "NUMBER(*,5)",
        "DECIMAL", "DECIMAL(20)", "DECIMAL(20,5)", "DECIMAL(20,-5)", "DECIMAL(*,5)"};
  }

  @Override
  public String[] getNames() {
    return new String[]{"ID", "N2", "N3", "N4", "N5", "D1", "D2", "D3", "D4", "D5"};
  }

  @Override
  public List<String[]> getSampleData() {
    List<String[]> data = new ArrayList<>();
    data.add(new String[]{"1", "1000000.05", "1000000.05", "1000000.05", "1000000.05",
        "100.02", "1000000.05", "1000000.05", "1000000.05", "1000000.05"});
    return data;
  }

  @Override
  public String[] getExpectedResultsForAvro() {
    String expectedRecord = "{\"ID\": 1, \"N2\": 1000000, \"N3\": 1000000.05000, \"N4\": 1000000, \"N5\": 1000000.05000, " +
        "\"D1\": 100, \"D2\": 1000000, \"D3\": 1000000.05000, \"D4\": 1000000, \"D5\": 1000000.05000}";
    String[] expectedResult = new String[1];
    expectedResult[0] = expectedRecord;
    return expectedResult;
  }

  @Override
  public String[] getExpectedResultsForParquet() {
    String expectedRecord = "1,1000000,1000000.05000,1000000,1000000.05000,100,1000000,1000000.05000,1000000,1000000.05000";
    String[] expectedResult = new String[1];
    expectedResult[0] = expectedRecord;
    return expectedResult;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
