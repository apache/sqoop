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

package org.apache.sqoop.manager.oracle;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OraOopDataDrivenDBInputFormatConnectionCloseTest {

  private static final OraOopLog LOG = OraOopLogFactory.getLog(
      TestOraOopDataDrivenDBInputFormat.class.getName());

  private static final String ORACLE_PREPARED_STATEMENT_CLASS = "oracle.jdbc.OraclePreparedStatement";

  private OraOopDataDrivenDBInputFormat inputFormat;

  private Connection mockConnection;

  private JobContext mockJobContext;

  @Before
  public void setUp() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(DBConfiguration.USERNAME_PROPERTY, "Oracle user");
    configuration.setInt(OraOopConstants.ORAOOP_DESIRED_NUMBER_OF_MAPPERS, 1);

    Class<? extends PreparedStatement> preparedStatementClass =
        (Class<? extends PreparedStatement>) Class.forName(ORACLE_PREPARED_STATEMENT_CLASS);
    PreparedStatement mockPreparedStatement = mock(preparedStatementClass);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

    mockConnection = mock(Connection.class);
    DatabaseMetaData dbMetaData = mock(DatabaseMetaData.class);
    when(dbMetaData.getDatabaseProductName()).thenReturn("Oracle");
    when(mockConnection.getMetaData()).thenReturn(dbMetaData);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

    DBConfiguration dbConf = mock(DBConfiguration.class);
    when(dbConf.getConnection()).thenReturn(mockConnection);
    when(dbConf.getConf()).thenReturn(configuration);
    when(dbConf.getInputTableName()).thenReturn("InputTable");

    mockJobContext = mock(JobContext.class);
    when(mockJobContext.getConfiguration()).thenReturn(configuration);

    inputFormat = new OraOopDataDrivenDBInputFormat();
    inputFormat.setDbConf(dbConf);
  }

  @Test
  public void testGetSplitsClosesConnectionProperly() throws Exception {
    inputFormat.getSplits(mockJobContext);
    verify(mockConnection).commit();
    verify(mockConnection).close();
  }

  @Test
  public void testGetSplitsClosesConnectionProperlyWhenExceptionIsThrown() throws Exception {

    doThrow(new SQLException("For the sake of testing the commit fails.")).when(mockConnection).commit();

    try {
      inputFormat.getSplits(mockJobContext);
    } catch (IOException e) {
      LOG.debug("An expected exception is thrown in testSplitsClosesConnectionProperlyWhenExceptionIsThrown, ignoring.");
    }

    verify(mockConnection).rollback();
    verify(mockConnection).close();

  }


}
