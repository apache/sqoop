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

package org.apache.sqoop;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Types;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.GenericJdbcManager;
import org.apache.sqoop.tool.ExportTool;
import org.h2.Driver;
import org.junit.After;
import org.junit.Before;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.TestExport;

/**
 * We'll use H2 as a database as the version of HSQLDB we currently depend on
 * (1.8) doesn't include support for stored procedures.
 */
public class TestExportUsingProcedure extends TestExport {
  private static final String PROCEDURE_NAME = "INSERT_PROCEDURE";
  /**
   * Stored procedures are static; we'll need an instance to get a connection.
   */
  private static TestExportUsingProcedure instanceForProcedure;
  private int functionCalls = 0;
  private String[] names;
  private String[] types;
  private Connection connection;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    instanceForProcedure = this;
  }

  @Override
  public void createTable(ColumnGenerator... extraColumns) throws SQLException {
    super.createTable(extraColumns);
    names = new String[extraColumns.length];
    types = new String[extraColumns.length];
    for (int i = 0; i < extraColumns.length; ++i) {
      names[i] = forIdx(i);
      types[i] = extraColumns[i].getType();
    }
    createProcedure(names, types);
  }

  protected void createProcedure(String[] extraNames, String[] extraTypes)
      throws SQLException {
    StringBuilder drop = new StringBuilder("DROP ALIAS IF EXISTS ");
    drop.append(PROCEDURE_NAME);

    StringBuilder create = new StringBuilder("CREATE ALIAS ");
    create.append(PROCEDURE_NAME);
    create.append(" FOR \"");
    create.append(getClass().getName());
    create.append(".insertFunction");
    if (extraNames.length > 0) {
      create.append(getName());
    }
    create.append('"');

    Connection conn = getConnection();
    Statement statement = conn.createStatement();
    try {
      statement.execute(drop.toString());
      statement.execute(create.toString());
      conn.commit();
    } finally {
      statement.close();
    }
  }

  @Override
  protected String getConnectString() {
    return "jdbc:h2:mem:" + getName();
  }

  @Override
  protected void verifyExport(int expectedNumRecords, Connection conn)
      throws IOException, SQLException {
    assertEquals("stored procedure must be called for each row",
        expectedNumRecords, functionCalls);
    super.verifyExport(expectedNumRecords, conn);
  }

  @Override
  protected String[] getArgv(boolean includeHadoopFlags, int rowsPerStmt,
      int statementsPerTx, String... additionalArgv) {
    // we need different class names per test, or the classloader will
    // just use the old class definition even though we've compiled a
    // new one!
    String[] args = newStrArray(additionalArgv, "--" + ExportTool.CALL_ARG,
        PROCEDURE_NAME, "--" + ExportTool.CLASS_NAME_ARG, getName(), "--"
            + ExportTool.CONN_MANAGER_CLASS_NAME,
        GenericJdbcManager.class.getName(), "--" + ExportTool.DRIVER_ARG,
        Driver.class.getName());
    return super
        .getArgv(includeHadoopFlags, rowsPerStmt, statementsPerTx, args);
  }

  @Override
  protected String[] getCodeGenArgv(String... extraArgs) {
    String[] myExtraArgs = newStrArray(extraArgs, "--"
        + ExportTool.CONN_MANAGER_CLASS_NAME,
        GenericJdbcManager.class.getName(), "--" + ExportTool.DRIVER_ARG,
        Driver.class.getName());
    return super.getCodeGenArgv(myExtraArgs);
  }

  @Override
  protected Connection getConnection() {
    if (connection != null) {
      return connection;
    }
    try {
      connection = DriverManager.getConnection(getConnectString());
      connection.setAutoCommit(false);
      return connection;
    } catch (SQLException e) {
      throw new AssertionError(e.getMessage());
    }
  }

  /**
   * This gets called during {@link #setUp()} to check the non-HSQLDB database
   * is valid. We'll therefore set the connection manager here...
   */
  @Override
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions ret = new SqoopOptions(conf);
    if (ret.getConnManagerClassName() == null) {
      ret.setConnManagerClassName(GenericJdbcManager.class.getName());
    }
    if (ret.getDriverClassName() == null) {
      ret.setDriverClassName(Driver.class.getName());
    }
    return ret;
  }

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Override
  protected boolean usesSQLtable() {
    return false;
  }

  @Override
  @After
  public void tearDown() {
    super.tearDown();
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // don't really care, it's only in memory
      }
    }
  }

  // TEST OVERRIDES

  @Override
  public void testMultiMapTextExportWithStaging() throws IOException,
      SQLException {
    try {
      super.testMultiMapTextExportWithStaging();
      fail("staging tables not compatible with --call");
    } catch (IOException e) {
      // expected
    }
  }

  @Override
  public void testMultiTransactionWithStaging() throws IOException,
      SQLException {
    try {
      super.testMultiTransactionWithStaging();
      fail("staging tables not compatible with --call");
    } catch (IOException e) {
      // expected
    }
  }

  /**
   * H2 renames the stored procedure arguments P1, P2, ..., Pn.
   */
  @Override
  public void testColumnsExport() throws IOException, SQLException {
    super.testColumnsExport("P1,P2,P3,P4");
  }

  // STORED PROCEDURES

  private interface SetExtraArgs {
    void set(PreparedStatement on) throws SQLException;
  }

  private static void insertFunction(int id, String msg,
      SetExtraArgs setExtraArgs) throws SQLException {
    instanceForProcedure.functionCalls += 1;
    Connection con = instanceForProcedure.getConnection();

    StringBuilder sql = new StringBuilder("insert into ");
    sql.append(instanceForProcedure.getTableName());
    sql.append("(id, msg");
    for (int i = 0; i < instanceForProcedure.names.length; ++i) {
      sql.append(", ");
      sql.append(instanceForProcedure.names[i]);
    }
    sql.append(") values (");
    for (int i = 0; i < instanceForProcedure.names.length + 2; ++i) {
      sql.append("?,");
    }
    // Remove last ,
    sql = sql.delete(sql.length() - 1, sql.length());
    sql.append(")");

    PreparedStatement statement = con.prepareStatement(sql.toString());
    try {
      statement.setInt(1, id);
      statement.setString(2, msg);
      setExtraArgs.set(statement);
      statement.execute();
      con.commit();
    } finally {
      statement.close();
    }
  }

  public static void insertFunction(int id, String msg) throws SQLException {
    insertFunction(id, msg, new SetExtraArgs() {
      @Override
      public void set(PreparedStatement on) throws SQLException {
      }
    });
  }

  public static void insertFunctiontestIntCol(int id, String msg,
      final int testIntCol) throws SQLException {
    insertFunction(id, msg, new SetExtraArgs() {
      @Override
      public void set(PreparedStatement on) throws SQLException {
        on.setInt(3, testIntCol);
      }
    });
  }

  public static void insertFunctiontestBigIntCol(int id, String msg,
      final long testBigIntCol) throws SQLException {
    insertFunction(id, msg, new SetExtraArgs() {
      @Override
      public void set(PreparedStatement on) throws SQLException {
        on.setLong(3, testBigIntCol);
      }
    });
  }

  public static void insertFunctiontestDatesAndTimes(int id, String msg,
      final Date date, final Time time) throws SQLException {
    insertFunction(id, msg, new SetExtraArgs() {
      @Override
      public void set(PreparedStatement on) throws SQLException {
        on.setDate(3, date);
        on.setTime(4, time);
      }
    });
  }

  public static void insertFunctiontestNumericTypes(int id, String msg,
      final BigDecimal f, final BigDecimal d) throws SQLException {
    insertFunction(id, msg, new SetExtraArgs() {
      @Override
      public void set(PreparedStatement on) throws SQLException {
        on.setBigDecimal(3, f);
        on.setBigDecimal(4, d);
      }
    });
  }

  /**
   * This test case is special - we're only inserting into a subset of the
   * columns in the table.
   */
  public static void insertFunctiontestColumnsExport(int id, String msg,
      final int int1, final int int2) throws SQLException {
    insertFunction(id, msg, new SetExtraArgs() {
      @Override
      public void set(PreparedStatement on) throws SQLException {
        on.setInt(3, int1);
        on.setNull(4, Types.INTEGER);
        on.setInt(5, int2);
      }
    });
  }

}
