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

package org.apache.sqoop.manager.hsqldb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.AvroTestUtils;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;


public class TestHsqldbAvroPadding extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      TestHsqldbAvroPadding.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    super.setUp();
    createTestTable();
  }

  protected void createTestTable() {
    String[] names = {"ID",  "NAME", "SALARY", "DEPT"};
    String[] types = { "INT", "VARCHAR(24)", "DECIMAL(20,5)", "VARCHAR(32)"};
    List<String[]> inputData = AvroTestUtils.getInputData();
    createTableWithColTypesAndNames(names, types, new String[0]);
    insertIntoTable(names, types, inputData.get(0));
    insertIntoTable(names, types, inputData.get(1));
    insertIntoTable(names, types, inputData.get(2));
  }

  protected ArgumentArrayBuilder getArgumentArrayBuilder() {
    ArgumentArrayBuilder builder = AvroTestUtils.getBuilderForAvroPaddingTest(this);
    builder.withOption("connect", getConnectString());
    return builder;
  }

  @Test
  public void testAvroImportWithoutPaddingFails() throws IOException {
    thrown.expect(IOException.class);
    thrown.expectMessage("Failure during job; return status 1");
    String[] args = getArgumentArrayBuilder().build();
    runImport(args);
  }

  @Test
  public void testAvroImportWithPadding() throws IOException {
    ArgumentArrayBuilder builder = getArgumentArrayBuilder();
    builder.withProperty("sqoop.avro.decimal_padding.enable", "true");
    String[] args = builder.build();
    runImport(args);
    AvroTestUtils.registerDecimalConversionUsageForVerification();
    AvroTestUtils.verify(AvroTestUtils.getExpectedResults(), getConf(), getTablePath());
  }
}
