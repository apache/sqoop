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
 * Numbers with a scale and precision greater that 38 are expected to work in Parquet and Avro import properly.
 *
 * With padding turned on, all of the numbers are expected to be padded with 0s, so that the total number of digits
 * after the decimal point will be equal to their scale.
 */
public class PostgresqlImportJobTestConfigurationPaddingShouldSucceed implements ImportJobTestConfiguration, AvroTestConfiguration, ParquetTestConfiguration, HiveTestConfiguration {

  @Override
  public String[] getTypes() {
    String[] columnTypes = {"INT", "NUMERIC(20)", "NUMERIC(20,5)", "NUMERIC(20,0)", "NUMERIC(1000,50)",
        "DECIMAL(20)", "DECIMAL(20)", "DECIMAL(20,5)", "DECIMAL(20,0)", "DECIMAL(1000,50)"};
    return columnTypes;
  }

  @Override
  public String[] getNames() {
    String[] columnNames = {"ID", "N2", "N3", "N4", "N5", "D1", "D2", "D3", "D4", "D5"};
    return columnNames;
  }

  @Override
  public List<String[]> getSampleData() {
    List<String[]> inputData = new ArrayList<>();
    inputData.add(new String[]{"1", "1000000.05", "1000000.05", "1000000.05", "1000000.05",
        "100.02", "1000000.05", "1000000.05", "1000000.05", "11111111112222222222333333333344444444445555555555.111111111122222222223333333333444444444455555"});
    return inputData;
  }

  @Override
  public String[] getExpectedResultsForAvro() {
    String expectedRecord = "{\"ID\": 1, \"N2\": 1000000, \"N3\": 1000000.05000, \"N4\": 1000000, \"N5\": 1000000.05000000000000000000000000000000000000000000000000, " +
        "\"D1\": 100, \"D2\": 1000000, \"D3\": 1000000.05000, \"D4\": 1000000, \"D5\": 11111111112222222222333333333344444444445555555555.11111111112222222222333333333344444444445555500000}";
    String[] expectedResult = new String[1];
    expectedResult[0] = expectedRecord;
    return expectedResult;
  }

  @Override
  public String[] getExpectedResultsForParquet() {
    String expectedRecord = "1,1000000,1000000.05000,1000000,1000000.05000000000000000000000000000000000000000000000000," +
        "100,1000000,1000000.05000,1000000,11111111112222222222333333333344444444445555555555.11111111112222222222333333333344444444445555500000";
    String[] expectedResult = new String[1];
    expectedResult[0] = expectedRecord;
    return expectedResult;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  /**
   * Special cases for numbers with a precision or scale higher than 38, i.e. the maximum precision and scale in Hive:
   * - parquet import will be successful, so data will be present on storage
   * - but Hive won't be able to read it, and returns null instead of objects.
   *
   * Because: Hive has an upper limit of 38 for both precision and scale and won't be able to read the numbers (returns null) above the limit.
   */
  @Override
  public Object[] getExpectedResultsForHive() {
    return new Object[]{
        new Integer(1),
        new BigDecimal("1000000"),
        new BigDecimal("1000000.05000"),
        new BigDecimal("1000000"),
        null,
        new BigDecimal("100"),
        new BigDecimal("1000000"),
        new BigDecimal("1000000.05000"),
        new BigDecimal("1000000"),
        null
    };
  }
}
