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

package org.apache.sqoop.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class TestHiveServer2Client {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final String CREATE_TABLE_STATEMENT = "createTableStatement";

  private static final String LOAD_DATA_STATEMENT = "loadDataStatement";

  private static final List<String> TEST_COMMANDS = asList("command1", "command2", "command3");

  private HiveServer2Client hs2Client;

  private HiveServer2Client hs2ClientSpy;

  private SqoopOptions sqoopOptions;

  private TableDefWriter tableDefWriter;

  private JdbcConnectionFactory hs2ConnectionFactory;

  private Connection hs2Connection;

  private PreparedStatement preparedStatement;

  private HiveClientCommon hiveClientCommon;

  private Path finalPath;

  private Configuration configuration;

  @Before
  public void before() throws Exception {
    sqoopOptions = mock(SqoopOptions.class);
    tableDefWriter = mock(TableDefWriter.class);
    hs2ConnectionFactory = mock(JdbcConnectionFactory.class);
    hs2Connection = mock(Connection.class);
    preparedStatement = mock(PreparedStatement.class);
    hiveClientCommon = mock(HiveClientCommon.class);
    finalPath = mock(Path.class);
    configuration = mock(Configuration.class);

    when(sqoopOptions.getConf()).thenReturn(configuration);
    when(hs2ConnectionFactory.createConnection()).thenReturn(hs2Connection);
    when(hs2Connection.prepareStatement(anyString())).thenReturn(preparedStatement);

    when(tableDefWriter.getCreateTableStmt()).thenReturn(CREATE_TABLE_STATEMENT);
    when(tableDefWriter.getLoadDataStmt()).thenReturn(LOAD_DATA_STATEMENT);
    when(tableDefWriter.getFinalPath()).thenReturn(finalPath);

    hs2Client = new HiveServer2Client(sqoopOptions, tableDefWriter, hs2ConnectionFactory, hiveClientCommon);
    hs2ClientSpy = spy(hs2Client);
  }

  @Test
  public void testImportTableExecutesHiveImportWithCreateTableAndLoadDataCommands() throws Exception {
    doNothing().when(hs2ClientSpy).executeHiveImport(anyList());

    hs2ClientSpy.importTable();

    verify(hs2ClientSpy, times(1)).executeHiveImport(asList(CREATE_TABLE_STATEMENT, LOAD_DATA_STATEMENT));
  }

  @Test
  public void testCreateTableExecutesHiveImportWithCreateTableCommandOnly() throws Exception {
    doNothing().when(hs2ClientSpy).executeHiveImport(anyList());

    hs2ClientSpy.createTable();

    verify(hs2ClientSpy, times(1)).executeHiveImport(asList(CREATE_TABLE_STATEMENT));
  }

  @Test
  public void testExecuteHiveImportInvokesMethodsInCorrectSequence() throws Exception {
    InOrder inOrder = Mockito.inOrder(hiveClientCommon, hs2ClientSpy);
    doNothing().when(hs2ClientSpy).executeCommands(TEST_COMMANDS);

    hs2ClientSpy.executeHiveImport(TEST_COMMANDS);

    inOrder.verify(hiveClientCommon).removeTempLogs(configuration, finalPath);
    inOrder.verify(hiveClientCommon).indexLzoFiles(sqoopOptions, finalPath);
    inOrder.verify(hs2ClientSpy).executeCommands(TEST_COMMANDS);
    inOrder.verify(hiveClientCommon).cleanUp(configuration, finalPath);
  }

  @Test
  public void testExecuteHiveImportThrowsRuntimeExceptionWhenExecuteCommandsThrows() throws Exception {
    SQLException sqlException = mock(SQLException.class);
    doThrow(sqlException).when(hs2ClientSpy).executeCommands(anyList());

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Error executing Hive import.");

    hs2ClientSpy.executeHiveImport(TEST_COMMANDS);
  }

  @Test
  public void testExecuteCommandsCreatesExactlyOneConnection() throws Exception {
    hs2Client.executeCommands(TEST_COMMANDS);

    verify(hs2ConnectionFactory, times(1)).createConnection();
  }

  @Test
  public void testExecuteCommandsClosesConnectionWhenStatementExecutionIsSuccessful() throws Exception {
    hs2Client.executeCommands(TEST_COMMANDS);

    verify(hs2Connection).close();
  }

  @Test
  public void testExecuteCommandsClosesConnectionWhenStatementExecutionThrows() throws Exception {
    when(hs2Connection.prepareStatement(anyString())).thenThrow(new SQLException());

    expectedException.expect(SQLException.class);
    hs2Client.executeCommands(TEST_COMMANDS);

    verify(hs2Connection).close();
  }

  @Test
  public void testExecuteCommandsClosesPreparedStatementsWhenStatementExecutionIsSuccessful() throws Exception {
    hs2Client.executeCommands(TEST_COMMANDS);

    verify(preparedStatement, times(TEST_COMMANDS.size())).close();
  }

  @Test
  public void testExecuteCommandsClosesPreparedStatementWhenStatementExecutionThrows() throws Exception {
    when(preparedStatement.execute()).thenThrow(new SQLException());

    expectedException.expect(SQLException.class);
    hs2Client.executeCommands(TEST_COMMANDS);

    verify(preparedStatement).close();
  }

  @Test
  public void testExecuteCommandsThrowsWhenCreateConnectionThrows() throws Exception {
    RuntimeException expected = mock(RuntimeException.class);
    when(hs2ConnectionFactory.createConnection()).thenThrow(expected);

    expectedException.expect(equalTo(expected));
    hs2Client.executeCommands(TEST_COMMANDS);
  }

  @Test
  public void testExecuteCommandsThrowsWhenPrepareStatementThrows() throws Exception {
    SQLException expected = mock(SQLException.class);
    when(hs2Connection.prepareStatement(anyString())).thenThrow(expected);

    expectedException.expect(equalTo(expected));
    hs2Client.executeCommands(TEST_COMMANDS);
  }

  @Test
  public void testExecuteCommandsThrowsWhenExecuteStatementThrows() throws Exception {
    SQLException expected = mock(SQLException.class);
    when(preparedStatement.execute()).thenThrow(expected);

    expectedException.expect(equalTo(expected));
    hs2Client.executeCommands(TEST_COMMANDS);
  }

}
