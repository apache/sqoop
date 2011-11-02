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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.manager.SqlManager;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;

/**
 * Test external connection factory classes on the classpath.
 * This adds a connection factory that will accept all connect strings,
 * but then promptly throws an error message if you try to import or
 * export a table.
 *
 * You'll need to specify the sqoop.connection.factories property
 * (either in conf/sqoop-default.xml) or on the command line with -D;
 * this should include the 'ExtConnFactoryTest.ExtFactory' class.
 *
 * Run with: src/scripts/run-perftest.sh ExtConnFactoryTest \
 *     (normal sqoop arguments)
 */
public final class ExtConnFactoryTest {
  private ExtConnFactoryTest() {
  }

  /**
   * A ConnManager that cannot satisfy any connection requests;
   * all operations fail with an error message.
   */
  public static class FailingManager extends SqlManager {

    public FailingManager(SqoopOptions options) {
      super(options);
    }

    @Override
    public Connection getConnection() throws SQLException {
      throw new SQLException("This manager cannot create a connection");
    }

    @Override
    public void importTable(ImportJobContext context)
        throws ImportException {
      throw new ImportException("This manager cannot read tables.");
    }

    @Override
    protected ResultSet execute(String stmt, Object... args)
        throws SQLException {
      throw new SQLException("This manager cannot execute SQL statements");
    }

    @Override
    public void exportTable(ExportJobContext context) throws ExportException {
      throw new ExportException("This manager cannot write tables.");
    }

    @Override
    public String getDriverClass() {
      return null;
    }
  }

  /**
   * Run the connection factory test.
   */
  public static void main(String [] args) throws Exception {
    Sqoop.main(args);
  }
}
