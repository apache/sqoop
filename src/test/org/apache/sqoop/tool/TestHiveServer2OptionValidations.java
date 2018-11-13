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

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.sqoop.SqoopOptions.IncrementalMode.None;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
@Category(UnitTest.class)
@Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
public class TestHiveServer2OptionValidations {

  @Parameters(name = "sqoopTool = {0}")
  public static Iterable<? extends Object> parameters() {
    return Arrays.asList(
        new ImportTool(),
        new ImportAllTablesTool(),
        new CreateHiveTableTool());
  }

  private static final String TEST_HS2_URL = "test-hs2-url";
  private static final String TEST_HS2_USER = "test-hs2-user";
  private static final String TEST_HS2_KEYTAB = "test-hs2-keytab";
  private static final String TEST_TABLE = "testtable";
  private static final String TEST_CONNECTION_STRING = "testconnectstring";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final BaseSqoopTool sqoopTool;

  private SqoopOptions sqoopOptions;

  public TestHiveServer2OptionValidations(BaseSqoopTool sqoopTool) {
    this.sqoopTool = spy(sqoopTool);
  }

  @Before
  public void before() {
    sqoopOptions = mock(SqoopOptions.class);
    when(sqoopOptions.getTableName()).thenReturn(TEST_TABLE);
    when(sqoopOptions.getIncrementalMode()).thenReturn(None);
    when(sqoopOptions.getConnectString()).thenReturn(TEST_CONNECTION_STRING);
    when(sqoopOptions.getMapColumnHive()).thenReturn(new Properties());


    doReturn(0).when(sqoopTool).getDashPosition(any(String[].class));
  }

  @Test
  public void testValidateOptionsThrowsWhenHs2UrlIsUsedWithoutHiveImport() throws Exception {
    expectedException.expect(SqoopOptions.InvalidOptionsException.class);
    expectedException.expectMessage("The hs2-url option cannot be used without the hive-import option.");

    when(sqoopOptions.doHiveImport()).thenReturn(false);
    when(sqoopOptions.getHs2Url()).thenReturn(TEST_HS2_URL);

    sqoopTool.validateOptions(sqoopOptions);
  }

  @Test
  public void testValidateOptionsThrowsWhenHs2UrlIsUsedWithHCatalogImport() throws Exception {
    expectedException.expect(SqoopOptions.InvalidOptionsException.class);
    expectedException.expectMessage("The hs2-url option cannot be used without the hive-import option.");

    when(sqoopOptions.getHCatTableName()).thenReturn(TEST_TABLE);
    when(sqoopOptions.getHs2Url()).thenReturn(TEST_HS2_URL);

    sqoopTool.validateOptions(sqoopOptions);
  }

  @Test
  public void testValidateOptionsThrowsWhenHs2UserIsUsedWithoutHs2Url() throws Exception {
    expectedException.expect(SqoopOptions.InvalidOptionsException.class);
    expectedException.expectMessage("The hs2-user option cannot be used without the hs2-url option.");

    when(sqoopOptions.getHs2User()).thenReturn(TEST_HS2_USER);

    sqoopTool.validateOptions(sqoopOptions);
  }

  @Test
  public void testValidateOptionsThrowsWhenHs2KeytabIsUsedWithoutHs2User() throws Exception {
    expectedException.expect(SqoopOptions.InvalidOptionsException.class);
    expectedException.expectMessage("The hs2-keytab option cannot be used without the hs2-user option.");

    when(sqoopOptions.getHs2Keytab()).thenReturn(TEST_HS2_KEYTAB);

    sqoopTool.validateOptions(sqoopOptions);
  }

  @Test
  public void testValidateOptionsSucceedsWhenHs2UrlIsUsedWithHiveImport() throws Exception {
    when(sqoopOptions.doHiveImport()).thenReturn(true);
    when(sqoopOptions.getHs2Url()).thenReturn(TEST_HS2_URL);

    sqoopTool.validateOptions(sqoopOptions);
  }

  @Test
  public void testValidateOptionsSucceedsWhenHs2UrlIsUsedWithHiveImportAndHs2UserButWithoutHs2Keytab() throws Exception {
    when(sqoopOptions.doHiveImport()).thenReturn(true);
    when(sqoopOptions.getHs2Url()).thenReturn(TEST_HS2_URL);
    when(sqoopOptions.getHs2User()).thenReturn(TEST_HS2_URL);

    sqoopTool.validateOptions(sqoopOptions);
  }

}
