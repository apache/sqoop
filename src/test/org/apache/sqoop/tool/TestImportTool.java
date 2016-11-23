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
import static org.junit.Assert.assertThat;

import java.sql.Connection;

import org.apache.sqoop.SqoopOptions;
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

  @Theory
  public void esnureTransactionIsolationLevelsAreMappedToTheRightValues(Object[] values)
      throws Exception {
    ImportTool importTool = new ImportTool();
    String[] args = { "--" + BaseSqoopTool.METADATA_TRANSACTION_ISOLATION_LEVEL, values[0].toString() };
    SqoopOptions options = importTool.parseArguments(args, null, null, true);
    assertThat(options.getMetadataTransactionIsolationLevel(), is(equalTo(values[1])));
  }

}
