/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.cloud.tools;

import static java.util.Arrays.asList;
import static org.apache.sqoop.testutil.BaseSqoopTestCase.timeFromString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CloudTestDataSet {

  private final String[] columnNames = {"ID", "SUPERHERO", "COMICS", "DEBUT"};
  private final String[] columnTypes = {"INT", "VARCHAR(25)", "VARCHAR(25)", "INT"};
  private final String[] columnNamesForMerge = {"DEBUT", "SUPERHERO1", "SUPERHERO2", "RECORD_DATE"};
  private final String[] columnTypesForMerge = {"INT", "VARCHAR(25)", "VARCHAR(25)", "TIMESTAMP"};
  private final String initialTimestampForMerge = "2018-07-23 15:00:00.000";
  private final String newTimestampForMerge = "2018-08-16 16:30:09.000";
  private final String expectedInitialTimestampForMerge = "2018-07-23 15:00:00.0";
  private final String expectedNewTimestampForMerge = "2018-08-16 16:30:09.0";

  public List<String[]> getInputData() {
    List<String[]> data = new ArrayList<>();
    data.add(new String[]{"1", "'Ironman'", "'Marvel'", "1963"});
    data.add(new String[]{"2", "'Wonder Woman'", "'DC'", "1941"});
    data.add(new String[]{"3", "'Batman'", "'DC'", "1939"});
    data.add(new String[]{"4", "'Hulk'", "'Marvel'", "1962"});
    return data;
  }

  public String[] getExtraInputData() {
    return new String[]{"5", "'Black Widow'", "'Marvel'", "1964"};
  }

  public List<List<Object>> getInitialInputDataForMerge() {
    return Arrays.asList(
        Arrays.asList(1940, "Black Widow", "Falcon", initialTimestampForMerge),
        Arrays.asList(1974, "Iron Fist", "The Punisher", initialTimestampForMerge));
  }

  public List<List<Object>> getNewInputDataForMerge() {
    return Arrays.asList(
        Arrays.asList(1962, "Spiderman", "Thor", newTimestampForMerge),
        Arrays.asList(1974, "Wolverine", "The Punisher", newTimestampForMerge));
  }

  public String[] getExpectedTextOutput() {
    return new String[]{
        "1,Ironman,Marvel,1963",
        "2,Wonder Woman,DC,1941",
        "3,Batman,DC,1939",
        "4,Hulk,Marvel,1962"
    };
  }

  public List<String> getExpectedTextOutputAsList() {
    return Arrays.asList(getExpectedTextOutput());
  }

  public String[] getExpectedExtraTextOutput() {
    return new String[]{
        "5,Black Widow,Marvel,1964"
    };
  }

  public String[] getExpectedTextOutputBeforeMerge() {
    return new String[]{
        "1940,Black Widow,Falcon," + expectedInitialTimestampForMerge,
        "1974,Iron Fist,The Punisher," + expectedInitialTimestampForMerge
    };
  }

  public String[] getExpectedTextOutputAfterMerge() {
    return new String[]{
        "1940,Black Widow,Falcon," + expectedInitialTimestampForMerge,
        "1962,Spiderman,Thor," + expectedNewTimestampForMerge,
        "1974,Wolverine,The Punisher," + expectedNewTimestampForMerge
    };
  }

  public String[] getExpectedSequenceFileOutput() {
    return new String[]{
        "1,Ironman,Marvel,1963\n",
        "2,Wonder Woman,DC,1941\n",
        "3,Batman,DC,1939\n",
        "4,Hulk,Marvel,1962\n"
    };
  }

  public String[] getExpectedExtraSequenceFileOutput() {
    return new String[]{
        "5,Black Widow,Marvel,1964\n"
    };
  }

  public String[] getExpectedAvroOutput() {
    return new String[]{
        "{\"ID\": 1, \"SUPERHERO\": \"Ironman\", \"COMICS\": \"Marvel\", \"DEBUT\": 1963}",
        "{\"ID\": 2, \"SUPERHERO\": \"Wonder Woman\", \"COMICS\": \"DC\", \"DEBUT\": 1941}",
        "{\"ID\": 3, \"SUPERHERO\": \"Batman\", \"COMICS\": \"DC\", \"DEBUT\": 1939}",
        "{\"ID\": 4, \"SUPERHERO\": \"Hulk\", \"COMICS\": \"Marvel\", \"DEBUT\": 1962}"
    };
  }

  public String[] getExpectedExtraAvroOutput() {
    return new String[]{
        "{\"ID\": 5, \"SUPERHERO\": \"Black Widow\", \"COMICS\": \"Marvel\", \"DEBUT\": 1964}"
    };
  }

  public List<String> getExpectedParquetOutput() {
    return asList(
        "1,Ironman,Marvel,1963",
        "2,Wonder Woman,DC,1941",
        "3,Batman,DC,1939",
        "4,Hulk,Marvel,1962");
  }

  public List<String> getExpectedParquetOutputAfterAppend() {
    return asList(
        "1,Ironman,Marvel,1963",
        "2,Wonder Woman,DC,1941",
        "3,Batman,DC,1939",
        "4,Hulk,Marvel,1962",
        "5,Black Widow,Marvel,1964");
  }

  public List<String> getExpectedParquetOutputWithTimestampColumn() {
    return asList(
        "1940,Black Widow,Falcon," + timeFromString(initialTimestampForMerge),
        "1974,Iron Fist,The Punisher," + timeFromString(initialTimestampForMerge));
  }

  public List<String> getExpectedParquetOutputWithTimestampColumnAfterMerge() {
    return asList(
        "1940,Black Widow,Falcon," + timeFromString(initialTimestampForMerge),
        "1962,Spiderman,Thor," + timeFromString(newTimestampForMerge),
        "1974,Wolverine,The Punisher," + timeFromString(newTimestampForMerge));
  }

  public String[] getColumnNames() {
    return columnNames;
  }

  public String[] getColumnTypes() {
    return columnTypes;
  }

  public String[] getColumnNamesForMerge() {
    return columnNamesForMerge;
  }

  public String[] getColumnTypesForMerge() {
    return columnTypesForMerge;
  }

  public String getInitialTimestampForMerge() {
    return initialTimestampForMerge;
  }
}
