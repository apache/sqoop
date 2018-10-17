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

package org.apache.sqoop;

import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.util.ParquetReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;

public class TestParquetIncrementalImportMerge extends ImportJobTestCase {

  private static final String[] TEST_COLUMN_TYPES = {"INTEGER", "VARCHAR(32)", "CHAR(64)", "TIMESTAMP"};

  private static final String[] ALTERNATIVE_TEST_COLUMN_TYPES = {"INTEGER", "INTEGER", "INTEGER", "TIMESTAMP"};

  private static final String INITIAL_RECORDS_TIMESTAMP = "2018-06-14 15:00:00.000";

  private static final String NEW_RECORDS_TIMESTAMP = "2018-06-14 16:00:00.000";

  private static final List<List<Object>> INITIAL_RECORDS = Arrays.<List<Object>>asList(
      Arrays.<Object>asList(2006, "Germany", "Italy", INITIAL_RECORDS_TIMESTAMP),
      Arrays.<Object>asList(2014, "Brazil", "Hungary", INITIAL_RECORDS_TIMESTAMP)
  );

  private static final List<Object> ALTERNATIVE_INITIAL_RECORD = Arrays.<Object>asList(1, 2, 3, INITIAL_RECORDS_TIMESTAMP);

  private static final List<List<Object>> NEW_RECORDS = Arrays.<List<Object>>asList(
      Arrays.<Object>asList(2010, "South Africa", "Spain", NEW_RECORDS_TIMESTAMP),
      Arrays.<Object>asList(2014, "Brazil", "Germany", NEW_RECORDS_TIMESTAMP)
  );

  private static final List<String> EXPECTED_MERGED_RECORDS = asList(
      "2006,Germany,Italy," + timeFromString(INITIAL_RECORDS_TIMESTAMP),
      "2010,South Africa,Spain," + timeFromString(NEW_RECORDS_TIMESTAMP),
      "2014,Brazil,Germany," + timeFromString(NEW_RECORDS_TIMESTAMP)
  );

  private static final List<String> EXPECTED_INITIAL_RECORDS = asList(
      "2006,Germany,Italy," + timeFromString(INITIAL_RECORDS_TIMESTAMP),
      "2014,Brazil,Hungary," + timeFromString(INITIAL_RECORDS_TIMESTAMP)
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Override
  public void setUp() {
    super.setUp();

    createTableWithRecords(TEST_COLUMN_TYPES, INITIAL_RECORDS);
  }

  @Test
  public void testSimpleMerge() throws Exception {
    String[] args = initialImportArgs(getConnectString(), getTableName(), getTablePath().toString()).build();
    runImport(args);

    clearTable(getTableName());

    insertRecordsIntoTable(TEST_COLUMN_TYPES, NEW_RECORDS);
    args = incrementalImportArgs(getConnectString(), getTableName(), getTablePath().toString(), getColName(3), getColName(0), INITIAL_RECORDS_TIMESTAMP).build();
    runImport(args);

    List<String> result = new ParquetReader(getTablePath()).readAllInCsvSorted();

    assertEquals(EXPECTED_MERGED_RECORDS, result);
  }

  @Test
  public void testMergeWhenTheIncrementalImportDoesNotImportAnyRows() throws Exception {
    String[] args = initialImportArgs(getConnectString(), getTableName(), getTablePath().toString()).build();
    runImport(args);

    clearTable(getTableName());

    args = incrementalImportArgs(getConnectString(), getTableName(), getTablePath().toString(), getColName(3), getColName(0), INITIAL_RECORDS_TIMESTAMP).build();
    runImport(args);

    List<String> result = new ParquetReader(getTablePath()).readAllInCsvSorted();

    assertEquals(EXPECTED_INITIAL_RECORDS, result);
  }

  @Test
  public void testMergeWithIncompatibleSchemas() throws Exception {
    String targetDir = getWarehouseDir() + "/testMergeWithIncompatibleSchemas";
    String[] args = initialImportArgs(getConnectString(), getTableName(), targetDir).build();
    runImport(args);

    incrementTableNum();
    createTableWithColTypes(ALTERNATIVE_TEST_COLUMN_TYPES, ALTERNATIVE_INITIAL_RECORD);

    args = incrementalImportArgs(getConnectString(), getTableName(), targetDir, getColName(3), getColName(0), INITIAL_RECORDS_TIMESTAMP).build();

    expectedException.expectMessage("Cannot merge files, the Avro schemas are not compatible.");
    runImportThrowingException(args);
  }

  @Test
  public void testMergedFilesHaveCorrectCodec() throws Exception {
    String[] args = initialImportArgs(getConnectString(), getTableName(), getTablePath().toString())
        .withOption("compression-codec", "snappy")
        .build();
    runImport(args);

    args = incrementalImportArgs(getConnectString(), getTableName(), getTablePath().toString(), getColName(3), getColName(0), INITIAL_RECORDS_TIMESTAMP)
        .withOption("compression-codec", "gzip")
        .build();
    runImport(args);

    CompressionCodecName compressionCodec = new ParquetReader(getTablePath()).getCodec();
    assertEquals(GZIP, compressionCodec);
  }

  private ArgumentArrayBuilder initialImportArgs(String connectString, String tableName, String targetDir) {
    return new ArgumentArrayBuilder()
        .withProperty("parquetjob.configurator.implementation", "hadoop")
        .withOption("connect", connectString)
        .withOption("table", tableName)
        .withOption("num-mappers", "1")
        .withOption("target-dir", targetDir)
        .withOption("as-parquetfile");
  }

  private ArgumentArrayBuilder incrementalImportArgs(String connectString, String tableName, String targetDir, String checkColumn, String mergeKey, String lastValue) {
    return initialImportArgs(connectString, tableName, targetDir)
        .withOption("incremental", "lastmodified")
        .withOption("check-column", checkColumn)
        .withOption("merge-key", mergeKey)
        .withOption("last-value", lastValue);
  }
}
