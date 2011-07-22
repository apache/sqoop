/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.manager;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.util.ImportException;

/**
 * Manages connections to MySQL databases.
 */
public class MySQLManager extends GenericJdbcManager {

  public static final Log LOG = LogFactory.getLog(MySQLManager.class.getName());

  // driver class to ensure is loaded when making db connection.
  private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

  // set to true after we warn the user that we can use direct fastpath.
  private static boolean warningPrinted = false;

  private Statement lastStatement;

  public MySQLManager(final SqoopOptions opts) {
    super(DRIVER_CLASS, opts);
  }

  protected MySQLManager(final SqoopOptions opts, boolean ignored) {
    // constructor used by subclasses to avoid the --direct warning.
    super(DRIVER_CLASS, opts);
  }

  @Override
  protected String getColNamesQuery(String tableName) {
    // Use mysql-specific hints and LIMIT to return fast
    return "SELECT t.* FROM " + escapeTableName(tableName) + " AS t LIMIT 1";
  }

  @Override
  public String[] listDatabases() {
    // TODO(aaron): Add an automated unit test for this.

    ResultSet results;
    try {
      results = execute("SHOW DATABASES");
    } catch (SQLException sqlE) {
      LOG.error("Error executing statement: " + sqlE.toString());
      release();
      return null;
    }

    try {
      ArrayList<String> databases = new ArrayList<String>();
      while (results.next()) {
        String dbName = results.getString(1);
        databases.add(dbName);
      }

      return databases.toArray(new String[0]);
    } catch (SQLException sqlException) {
      LOG.error("Error reading from database: " + sqlException.toString());
      return null;
    } finally {
      try {
        results.close();
      } catch (SQLException sqlE) {
        LOG.warn("Exception closing ResultSet: " + sqlE.toString());
      }

      release();
    }
  }

  @Override
  public void importTable(ImportJobContext context)
      throws IOException, ImportException {

    // Check that we're not doing a MapReduce from localhost. If we are, point
    // out that we could use mysqldump.
    if (!MySQLManager.warningPrinted) {
      String connectString = context.getOptions().getConnectString();

      if (null != connectString) {
        // DirectMySQLManager will probably be faster.
        LOG.warn("It looks like you are importing from mysql.");
        LOG.warn("This transfer can be faster! Use the --direct");
        LOG.warn("option to exercise a MySQL-specific fast path.");

        MySQLManager.markWarningPrinted(); // don't display this twice.
      }
    }

    checkDateTimeBehavior(context);

    // Then run the normal importTable() method.
    super.importTable(context);
  }

  /**
   * Set a flag to prevent printing the --direct warning twice.
   */
  protected static void markWarningPrinted() {
    MySQLManager.warningPrinted = true;
  }

  /**
   * MySQL allows TIMESTAMP fields to have the value '0000-00-00 00:00:00',
   * which causes errors in import. If the user has not set the
   * zeroDateTimeBehavior property already, we set it for them to coerce
   * the type to null.
   */
  private void checkDateTimeBehavior(ImportJobContext context) {
    final String ZERO_BEHAVIOR_STR = "zeroDateTimeBehavior";
    final String CONVERT_TO_NULL = "=convertToNull";

    String connectStr = context.getOptions().getConnectString();
    if (connectStr.indexOf("jdbc:") != 0) {
      // This connect string doesn't have the prefix we expect.
      // We can't parse the rest of it here.
      return;
    }

    // This starts with 'jdbc:mysql://' ... let's remove the 'jdbc:'
    // prefix so that java.net.URI can parse the rest of the line.
    String uriComponent = connectStr.substring(5);
    try {
      URI uri = new URI(uriComponent);
      String query = uri.getQuery(); // get the part after a '?'

      // If they haven't set the zeroBehavior option, set it to
      // squash-null for them.
      if (null == query) {
        connectStr = connectStr + "?" + ZERO_BEHAVIOR_STR + CONVERT_TO_NULL;
        LOG.info("Setting zero DATETIME behavior to convertToNull (mysql)");
      } else if (query.length() == 0) {
        connectStr = connectStr + ZERO_BEHAVIOR_STR + CONVERT_TO_NULL;
        LOG.info("Setting zero DATETIME behavior to convertToNull (mysql)");
      } else if (query.indexOf(ZERO_BEHAVIOR_STR) == -1) {
        if (!connectStr.endsWith("&")) {
          connectStr = connectStr + "&";
        }
        connectStr = connectStr + ZERO_BEHAVIOR_STR + CONVERT_TO_NULL;
        LOG.info("Setting zero DATETIME behavior to convertToNull (mysql)");
      }

      LOG.debug("Rewriting connect string to " + connectStr);
      context.getOptions().setConnectString(connectStr);
    } catch (URISyntaxException use) {
      // Just ignore this. If we can't parse the URI, don't attempt
      // to add any extra flags to it.
      LOG.debug("mysql: Couldn't parse connect str in checkDateTimeBehavior: "
          + use);
    }
  }

  /**
   * Executes an arbitrary SQL statement. Sets mysql-specific parameter
   * to ensure the entire table is not buffered in RAM before reading
   * any rows. A consequence of this is that every ResultSet returned
   * by this method *MUST* be close()'d, or read to exhaustion before
   * another query can be executed from this ConnManager instance.
   *
   * @param stmt The SQL statement to execute
   * @return A ResultSet encapsulating the results or null on error
   */
  protected ResultSet execute(String stmt, Object... args)
      throws SQLException {
    // Free any previous resources.
    release();

    PreparedStatement statement = null;
    statement = this.getConnection().prepareStatement(stmt,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    this.lastStatement = statement;
    statement.setFetchSize(Integer.MIN_VALUE); // MySQL: read row-at-a-time.
    if (null != args) {
      for (int i = 0; i < args.length; i++) {
        statement.setObject(i + 1, args[i]);
      }
    }

    LOG.info("Executing SQL statement: " + stmt);
    return statement.executeQuery();
  }

  @Override
  public void execAndPrint(String s) {
    // Override default execAndPrint() with a special version that forces
    // use of fully-buffered ResultSets (MySQLManager uses streaming ResultSets 
    // in the default execute() method; but the execAndPrint() method needs to
    // issue overlapped queries for metadata.)

    ResultSet results = null;
    try {
      // Use default execute() statement which does not issue the
      // MySQL-specific setFetchSize() command.
      results = super.execute(s);
    } catch (SQLException sqlE) {
      LOG.error("Error executing statement: "
          + StringUtils.stringifyException(sqlE));
      release();
      return;
    }

    PrintWriter pw = new PrintWriter(System.out, true);
    try {
      formatAndPrintResultSet(results, pw);
    } finally {
      pw.close();
    }
  }

  public void release() {
    if (null != this.lastStatement) {
      try {
        this.lastStatement.close();
      } catch (SQLException e) {
        LOG.warn("Exception closing executed Statement: " + e);
      }

      this.lastStatement = null;
    }
  }

  /**
   * When using a column name in a generated SQL query, how (if at all)
   * should we escape that column name? e.g., a column named "table"
   * may need to be quoted with backtiks: "`table`".
   *
   * @param colName the column name as provided by the user, etc.
   * @return how the column name should be rendered in the sql text.
   */
  public String escapeColName(String colName) {
    if (null == colName) {
      return null;
    }
    return "`" + colName + "`";
  }

  /**
   * When using a table name in a generated SQL query, how (if at all)
   * should we escape that column name? e.g., a table named "table"
   * may need to be quoted with backtiks: "`table`".
   *
   * @param tableName the table name as provided by the user, etc.
   * @return how the table name should be rendered in the sql text.
   */
  public String escapeTableName(String tableName) {
    if (null == tableName) {
      return null;
    }
    return "`" + tableName + "`";
  }
}

