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

package org.apache.sqoop.tool;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.Connection;

import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.hive.HiveImport;
import org.apache.avro.Schema;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.avro.AvroSchemaMismatchException;
import org.apache.sqoop.util.ExpectedLogMessage;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class TestImportTool {
  @DataPoints
  public static final Object[][] TRANSACTION_ISOLATION_LEVEL_NAMES_AND_VALUES = {
    {"TRANSACTION_NONE", Connection.TRANSACTION_NONE},
    {"TRANSACTION_READ_COMMITTED",Connection.TRANSACTION_READ_COMMITTED},
    {"TRANSACTION_READ_UNCOMMITTED",Connection.TRANSACTION_READ_UNCOMMITTED},
    {"TRANSACTION_REPEATABLE_READ",Connection.TRANSACTION_REPEATABLE_READ},
    {"TRANSACTION_SERIALIZABLE",Connection.TRANSACTION_SERIALIZABLE}
  };

  @Rule
  public ExpectedLogMessage logMessage = new ExpectedLogMessage();

  @Theory
  public void esnureTransactionIsolationLevelsAreMappedToTheRightValues(Object[] values)
      throws Exception {
    ImportTool importTool = new ImportTool();
    String[] args = { "--" + BaseSqoopTool.METADATA_TRANSACTION_ISOLATION_LEVEL, values[0].toString() };
    SqoopOptions options = importTool.parseArguments(args, null, null, true);
    assertThat(options.getMetadataTransactionIsolationLevel(), is(equalTo(values[1])));
  }

  @Test
  public void testImportToolHandlesAvroSchemaMismatchExceptionProperly() throws Exception {
    final String writtenWithSchemaString = "writtenWithSchema";
    final String actualSchemaString = "actualSchema";
    final String errorMessage = "Import failed";

    ImportTool importTool = spy(new ImportTool("import", mock(CodeGenTool.class), false));

    doReturn(true).when(importTool).init(any(com.cloudera.sqoop.SqoopOptions.class));

    Schema writtenWithSchema = mock(Schema.class);
    when(writtenWithSchema.toString()).thenReturn(writtenWithSchemaString);
    Schema actualSchema = mock(Schema.class);
    when(actualSchema.toString()).thenReturn(actualSchemaString);

    AvroSchemaMismatchException expectedException = new AvroSchemaMismatchException(errorMessage, writtenWithSchema, actualSchema);
    doThrow(expectedException).when(importTool).importTable(any(com.cloudera.sqoop.SqoopOptions.class), anyString(), any(HiveImport.class));

    com.cloudera.sqoop.SqoopOptions sqoopOptions = mock(com.cloudera.sqoop.SqoopOptions.class);
    when(sqoopOptions.doHiveImport()).thenReturn(true);

    logMessage.expectError(expectedException.getMessage());
    int result = importTool.run(sqoopOptions);
    assertEquals(1, result);
  }

  // If --external-table-dir is set and --hive-import is not, check an exception
  // is thrown
  @Test (expected = InvalidOptionsException.class)
  public void testExternalTableNoHiveImportThrowsException() throws InvalidOptionsException {
    String hdfsTableDir = "/data/movielens/genre";
    com.cloudera.sqoop.SqoopOptions options = new com.cloudera.sqoop.SqoopOptions("jdbc:postgresql://localhost/movielens", "genres");
    options.setHiveExternalTableDir(hdfsTableDir);
    ImportTool tool = new ImportTool("Import Tool", false);
    tool.validateHiveOptions(options);
    Assert.fail("testExternalTableNoHiveImportThrowsException unit test failed!");
  }

}
