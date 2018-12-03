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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * A note on the expected values here:
 *
 * With padding turned on, all of the numbers are expected to be padded with 0s, so that the total number of digits
 * after the decimal point will be equal to their scale.
 */
public class MysqlImportJobTestConfiguration implements ImportJobTestConfiguration, AvroTestConfiguration, ParquetTestConfiguration, HiveTestConfiguration {

  @Override
  public String[] getTypes() {
    String[] columnTypes = {"INT", "NUMERIC", "NUMERIC(20)", "NUMERIC(20,5)", "NUMERIC(20,0)", "NUMERIC(65,5)",
        "DECIMAL", "DECIMAL(20)", "DECIMAL(20,5)", "DECIMAL(20,0)", "DECIMAL(65,5)"};
    return columnTypes;
  }

  @Override
  public String[] getNames() {
    String[] columnNames = {"ID", "N1", "N2", "N3", "N4", "N5", "D1", "D2", "D3", "D4", "D5"};
    return columnNames;
  }

  @Override
  public List<String[]> getSampleData() {
    List<String[]> inputData = new ArrayList<>();
    inputData.add(new String[]{"1", "100.030", "1000000.05", "1000000.05", "1000000.05", "1000000.05",
        "100.040", "1000000.05", "1000000.05", "1000000.05", "11111111112222222222333333333344444444445555555555.05"});
    return inputData;
  }

  @Override
  public String[] getExpectedResultsForAvro() {
    String expectedRecord = "{\"ID\": 1, \"N1\": 100, \"N2\": 1000000, \"N3\": 1000000.05000, \"N4\": 1000000, \"N5\": 1000000.05000, " +
        "\"D1\": 100, \"D2\": 1000000, \"D3\": 1000000.05000, \"D4\": 1000000, \"D5\": 11111111112222222222333333333344444444445555555555.05000}";
    String[] expectedResult = new String[1];
    expectedResult[0] = expectedRecord;
    return expectedResult;
  }

  @Override
  public String[] getExpectedResultsForParquet() {
    String expectedRecord = "1,100,1000000,1000000.05000,1000000,1000000.05000,100,1000000,1000000.05000,1000000,11111111112222222222333333333344444444445555555555.05000";
    String[] expectedResult = new String[1];
    expectedResult[0] = expectedRecord;
    return expectedResult;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  /**
   * Since Mysql permits a precision that is higher than 65, there is a special test case here for the last column:
   * - parquet and avro import will be successful, so data will be present on storage
   * - but Hive won't be able to read it, and returns null.
   */
  @Override
  public Object[] getExpectedResultsForHive() {
    return new Object[]{
        new Integer(1),
        new BigDecimal("100"),
        new BigDecimal("1000000"),
        new BigDecimal("1000000.05000"),
        new BigDecimal("1000000"),
        new BigDecimal("1000000.05000"),
        new BigDecimal("100"),
        new BigDecimal("1000000"),
        new BigDecimal("1000000.05000"),
        new BigDecimal("1000000"),
        null
    };
  }
}
