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
package org.apache.sqoop.repository.derby;

import junit.framework.TestCase;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.framework.FrameworkManager;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import static org.apache.sqoop.repository.derby.DerbySchemaQuery.*;

/**
 * Abstract class with convenience methods for testing derby repository.
 */
abstract public class DerbyTestCase extends TestCase {

  private static int SYSTEM_VERSION = 4;

  public static final String DERBY_DRIVER =
    "org.apache.derby.jdbc.EmbeddedDriver";

  public static final String JDBC_URL =
    "jdbc:derby:memory:myDB";

  private Connection connection;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // Create connection to the database
    Class.forName(DERBY_DRIVER).newInstance();
    connection = DriverManager.getConnection(getStartJdbcUrl());
  }

  @Override
  public void tearDown() throws Exception {
    // Close active connection
    if(connection != null) {
      connection.close();
    }

    try {
      // Drop in memory database
      DriverManager.getConnection(getStopJdbcUrl());
    } catch (SQLException ex) {
      // Dropping Derby database leads always to exception
    }

    // Call parent tear down
    super.tearDown();
  }

  /**
   * Create derby schema.
   *
   * @throws Exception
   */
  protected void createSchema(int version) throws Exception {
    if (version > 0) {
      runQuery(QUERY_CREATE_SCHEMA_SQOOP);
      runQuery(QUERY_CREATE_TABLE_SQ_CONNECTOR);
      runQuery(QUERY_CREATE_TABLE_SQ_FORM);
      runQuery(QUERY_CREATE_TABLE_SQ_INPUT);
      runQuery(QUERY_CREATE_TABLE_SQ_CONNECTION);
      runQuery(QUERY_CREATE_TABLE_SQ_JOB);
      runQuery(QUERY_CREATE_TABLE_SQ_CONNECTION_INPUT);
      runQuery(QUERY_CREATE_TABLE_SQ_JOB_INPUT);
      runQuery(QUERY_CREATE_TABLE_SQ_SUBMISSION);
      runQuery(QUERY_CREATE_TABLE_SQ_COUNTER_GROUP);
      runQuery(QUERY_CREATE_TABLE_SQ_COUNTER);
      runQuery(QUERY_CREATE_TABLE_SQ_COUNTER_SUBMISSION);
    }

    if (version > 1) {
      runQuery(QUERY_CREATE_TABLE_SQ_SYSTEM);
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_ENABLED);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_ENABLED);
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_CREATION_USER);
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_UPDATE_USER);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_CREATION_USER);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_UPDATE_USER);
      runQuery(QUERY_UPGRADE_TABLE_SQ_SUBMISSION_ADD_COLUMN_CREATION_USER);
      runQuery(QUERY_UPGRADE_TABLE_SQ_SUBMISSION_ADD_COLUMN_UPDATE_USER);
    }

    if (version > 3) {
      runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_RENAME_COLUMN_SQF_OPERATION_TO_SQF_DIRECTION);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_RENAME_COLUMN_SQB_CONNECTION_TO_SQB_FROM_CONNECTION);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_SQB_TO_CONNECTION);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_REMOVE_CONSTRAINT_SQB_SQN);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_CONSTRAINT_SQB_SQN_FROM);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_CONSTRAINT_SQB_SQN_TO);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_REMOVE_COLUMN_SQB_TYPE);
    }

    runQuery("INSERT INTO SQOOP.SQ_SYSTEM(SQM_KEY, SQM_VALUE) VALUES('version', '"  + version + "')");
    runQuery("INSERT INTO SQOOP.SQ_SYSTEM(SQM_KEY, SQM_VALUE) " +
      "VALUES('framework.version', '1')");
  }

  protected void createSchema() throws Exception {
    createSchema(SYSTEM_VERSION);
  }

  /**
   * Run arbitrary query on derby memory repository.
   *
   * @param query Query to execute
   * @throws Exception
   */
  protected void runQuery(String query, String... args) throws Exception {
    PreparedStatement stmt = null;
    try {
      stmt = getDerbyConnection().prepareStatement(query);

      for (int i = 0; i < args.length; ++i) {
        stmt.setString(i + 1, args[i]);
      }

      stmt.execute();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  protected Connection getDerbyConnection() {
    return connection;
  }

  protected String getJdbcUrl() {
    return JDBC_URL;
  }

  protected String getStartJdbcUrl() {
    return JDBC_URL + ";create=true";
  }

  protected String getStopJdbcUrl() {
    return JDBC_URL + ";drop=true";
  }

  protected void loadConnectorAndFrameworkVersion2() throws Exception {
    // Connector entry
    runQuery("INSERT INTO SQOOP.SQ_CONNECTOR(SQC_NAME, SQC_CLASS, SQC_VERSION)"
        + "VALUES('A', 'org.apache.sqoop.test.A', '1.0-test')");

    String connector = "1";

    // Connector form entries
    for(String operation : new String[] {"null", "'IMPORT'", "'EXPORT'"}) {

      String type;
      if(operation.equals("null")) {
        type = "CONNECTION";
      } else {
        type = "JOB";
      }

      runQuery("INSERT INTO SQOOP.SQ_FORM"
          + "(SQF_CONNECTOR, SQF_OPERATION, SQF_NAME, SQF_TYPE, SQF_INDEX) "
          + "VALUES("
          + connector  + ", "
          + operation
          + ", 'F1', '"
          + type
          + "', 0)");
      runQuery("INSERT INTO SQOOP.SQ_FORM"
          + "(SQF_CONNECTOR, SQF_OPERATION, SQF_NAME, SQF_TYPE, SQF_INDEX) "
          + "VALUES("
          + connector + ", "
          + operation
          +  ", 'F2', '"
          + type
          + "', 1)");
    }

    // Framework form entries
    runQuery("INSERT INTO SQOOP.SQ_FORM"
        + "(SQF_CONNECTOR, SQF_OPERATION, SQF_NAME, SQF_TYPE, SQF_INDEX) VALUES"
        + "(NULL, 'IMPORT', 'output', 'JOB', 0),"
        + "(NULL, 'IMPORT', 'throttling', 'JOB', 1),"
        + "(NULL, 'EXPORT', 'input', 'JOB', 0),"
        + "(NULL, 'EXPORT', 'throttling', 'JOB', 1),"
        + "(NULL, NULL, 'security', 'CONNECTION', 0)");

    // Connector input entries
    int x = 0;
    for(int i = 0; i < 3; i++) {
      // First form
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          +"(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I1', " + (i * 2 + 1) + ", 0, 'STRING', false, 30)");
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          +"(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I2', " + (i * 2 + 1) + ", 1, 'MAP', false, 30)");

      // Second form
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          +"(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I3', " + (i * 2 + 2) + ", 0, 'STRING', false, 30)");
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          +"(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I4', " + (i * 2 + 2) + ", 1, 'MAP', false, 30)");
    }

    // Framework input entries.
    runQuery("INSERT INTO SQOOP.SQ_INPUT (SQI_NAME, SQI_FORM, SQI_INDEX,"
        + " SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_ENUMVALS)"
        +" VALUES ('security.maxConnections',11,0,'INTEGER','false',NULL,NULL),"
        + "('input.inputDirectory',9,0,'STRING','false',255,NULL),"
        + "('throttling.extractors',8,0,'INTEGER','false',NULL,NULL),"
        + "('throttling.loaders',8,1,'INTEGER','false',NULL,NULL),"
        + "('output.storageType',7,0,'ENUM','false',NULL,'HDFS'),"
        + "('output.outputFormat',7,1,'ENUM','false',NULL,'TEXT_FILE,SEQUENCE_FILE'),"
        + "('output.compression',7,2,'ENUM','false',NULL,'NONE,DEFAULT,DEFLATE,GZIP,BZIP2,LZO,LZ4,SNAPPY'),"
        + "('output.outputDirectory',7,3,'STRING','false',255,NULL),"
        + "('throttling.extractors',10,0,'INTEGER','false',NULL,NULL),"
        + "('throttling.loaders',10,1,'INTEGER','false',NULL,NULL)");
  }

  protected void loadConnectorAndFrameworkVersion4() throws Exception {
    // Connector entry
    runQuery("INSERT INTO SQOOP.SQ_CONNECTOR(SQC_NAME, SQC_CLASS, SQC_VERSION)"
        + "VALUES('A', 'org.apache.sqoop.test.A', '1.0-test')");

    // Connector part
    for (String connector : new String[]{"1"}) {
      // Form entries
      for (String direction : new String[]{"null", "'FROM'", "'TO'"}) {

        String type;
        if (direction.equals("null")) {
          type = "CONNECTION";
        } else {
          type = "JOB";
        }

        runQuery("INSERT INTO SQOOP.SQ_FORM"
            + "(SQF_CONNECTOR, SQF_DIRECTION, SQF_NAME, SQF_TYPE, SQF_INDEX) "
            + "VALUES("
            + connector + ", "
            + direction
            + ", 'F1', '"
            + type
            + "', 0)");
        runQuery("INSERT INTO SQOOP.SQ_FORM"
            + "(SQF_CONNECTOR, SQF_DIRECTION, SQF_NAME, SQF_TYPE, SQF_INDEX) "
            + "VALUES("
            + connector + ", "
            + direction
            + ", 'F2', '"
            + type
            + "', 1)");
      }
    }

    // Framework part
    for (String type : new String[]{"CONNECTION", "JOB"}) {
      runQuery("INSERT INTO SQOOP.SQ_FORM"
          + "(SQF_CONNECTOR, SQF_DIRECTION, SQF_NAME, SQF_TYPE, SQF_INDEX) "
          + "VALUES(NULL, NULL"
          + ", 'F1', '"
          + type
          + "', 0)");
      runQuery("INSERT INTO SQOOP.SQ_FORM"
          + "(SQF_CONNECTOR, SQF_DIRECTION, SQF_NAME, SQF_TYPE, SQF_INDEX) "
          + "VALUES(NULL, NULL"
          + ", 'F2', '"
          + type
          + "', 1)");
    }

    // Input entries
    // Connector connection parts: 0-3
    // Connector job (FROM) parts: 4-7
    // Connector job (TO) parts: 8-11
    // Framework connection parts: 12-15
    // Framework job parts: 16-19
    for (int i = 0; i < 5; i++) {
      // First form
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I1', " + (i * 2 + 1) + ", 0, 'STRING', false, 30)");
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I2', " + (i * 2 + 1) + ", 1, 'MAP', false, 30)");

      // Second form
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I3', " + (i * 2 + 2) + ", 0, 'STRING', false, 30)");
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I4', " + (i * 2 + 2) + ", 1, 'MAP', false, 30)");
    }
  }

  /**
   * Load testing connector and framework metadata into repository.
   *
   * @param version system version (2 or 4)
   * @throws Exception
   */
  protected void loadConnectorAndFramework(int version) throws Exception {
    switch(version) {
      case 2:
        loadConnectorAndFrameworkVersion2();
        break;

      case 4:
        loadConnectorAndFrameworkVersion4();
        break;

      default:
        throw new AssertionError("Invalid connector and framework version: " + version);
    }
  }

  protected void loadConnectorAndFramework() throws Exception {
    loadConnectorAndFramework(SYSTEM_VERSION);
  }

  /**
   * Load testing connection objects into metadata repository.
   *
   * @param version system version (2 or 4)
   * @throws Exception
   */
  public void loadConnections(int version) throws Exception {
    switch (version) {
      case 2:
        // Insert two connections - CA and CB
        runQuery("INSERT INTO SQOOP.SQ_CONNECTION(SQN_NAME, SQN_CONNECTOR) "
            + "VALUES('CA', 1)");
        runQuery("INSERT INTO SQOOP.SQ_CONNECTION(SQN_NAME, SQN_CONNECTOR) "
            + "VALUES('CB', 1)");

        for(String ci : new String[] {"1", "2"}) {
          for(String i : new String[] {"1", "3", "13", "15"}) {
            runQuery("INSERT INTO SQOOP.SQ_CONNECTION_INPUT"
                + "(SQNI_CONNECTION, SQNI_INPUT, SQNI_VALUE) "
                + "VALUES(" + ci + ", " + i + ", 'Value" + i + "')");
          }
        }
        break;

      case 4:
        // Insert two connections - CA and CB
        runQuery("INSERT INTO SQOOP.SQ_CONNECTION(SQN_NAME, SQN_CONNECTOR) "
            + "VALUES('CA', 1)");
        runQuery("INSERT INTO SQOOP.SQ_CONNECTION(SQN_NAME, SQN_CONNECTOR) "
            + "VALUES('CB', 1)");

        for (String ci : new String[]{"1", "2"}) {
          for (String i : new String[]{"1", "3", "13", "15"}) {
            runQuery("INSERT INTO SQOOP.SQ_CONNECTION_INPUT"
                + "(SQNI_CONNECTION, SQNI_INPUT, SQNI_VALUE) "
                + "VALUES(" + ci + ", " + i + ", 'Value" + i + "')");
          }
        }
        break;

      default:
        throw new AssertionError("Invalid connector and framework version: " + version);
    }
  }

  public void loadConnections() throws Exception {
    loadConnections(SYSTEM_VERSION);
  }

  /**
   * Load testing job objects into metadata repository.
   *
   * @param version system version (2 or 4)
   * @throws Exception
   */
  public void loadJobs(int version) throws Exception {
    switch (version) {
      case 2:
        for(String type : new String[] {"IMPORT", "EXPORT"}) {
          for(String name : new String[] {"JA", "JB"} ) {
            runQuery("INSERT INTO SQOOP.SQ_JOB(SQB_NAME, SQB_CONNECTION, SQB_TYPE)"
                + " VALUES('" + name + "', 1, '" + type + "')");
          }
        }

        // Import inputs
        for(String ci : new String[] {"1", "2"}) {
          for(String i : new String[] {"5", "7", "17", "19"}) {
            runQuery("INSERT INTO SQOOP.SQ_JOB_INPUT"
                + "(SQBI_JOB, SQBI_INPUT, SQBI_VALUE) "
                + "VALUES(" + ci + ", " + i + ", 'Value" + i + "')");
          }
        }

        // Export inputs
        for(String ci : new String[] {"3", "4"}) {
          for(String i : new String[] {"9", "11"}) {
            runQuery("INSERT INTO SQOOP.SQ_JOB_INPUT"
                + "(SQBI_JOB, SQBI_INPUT, SQBI_VALUE) "
                + "VALUES(" + ci + ", " + i + ", 'Value" + i + "')");
          }
        }
        break;


      case 4:
        for (String name : new String[]{"JA", "JB", "JC", "JD"}) {
          runQuery("INSERT INTO SQOOP.SQ_JOB(SQB_NAME, SQB_FROM_CONNECTION, SQB_TO_CONNECTION)"
              + " VALUES('" + name + "', 1, 1)");
        }

        // Odd IDs inputs have values
        for (String ci : new String[]{"1", "2", "3", "4"}) {
          for (String i : new String[]{"5", "9", "17"}) {
            runQuery("INSERT INTO SQOOP.SQ_JOB_INPUT"
                + "(SQBI_JOB, SQBI_INPUT, SQBI_VALUE) "
                + "VALUES(" + ci + ", " + i + ", 'Value" + i + "')");
          }

          for (String i : new String[]{"7", "11", "19"}) {
            runQuery("INSERT INTO SQOOP.SQ_JOB_INPUT"
                + "(SQBI_JOB, SQBI_INPUT, SQBI_VALUE) "
                + "VALUES(" + ci + ", " + i + ", 'Value" + i + "')");
          }
        }
        break;

      default:
        throw new AssertionError("Invalid connector and framework version: " + version);
    }
  }

  public void loadJobs() throws Exception {
    loadJobs(SYSTEM_VERSION);
  }

  /**
   * Add a second connector for testing with multiple connectors
   */
  public void addConnector() throws Exception {
    // Connector entry
    runQuery("INSERT INTO SQOOP.SQ_CONNECTOR(SQC_NAME, SQC_CLASS, SQC_VERSION)"
            + "VALUES('B', 'org.apache.sqoop.test.B', '1.0-test')");
  }

  /**
   * Load testing submissions into the metadata repository.
   *
   * @throws Exception
   */
  public void loadSubmissions() throws  Exception {
    runQuery("INSERT INTO SQOOP.SQ_COUNTER_GROUP "
      + "(SQG_NAME) "
      + "VALUES"
      + "('gA'), ('gB')"
    );

    runQuery("INSERT INTO SQOOP.SQ_COUNTER "
      + "(SQR_NAME) "
      + "VALUES"
      + "('cA'), ('cB')"
    );

    runQuery("INSERT INTO SQOOP.SQ_SUBMISSION"
      + "(SQS_JOB, SQS_STATUS, SQS_CREATION_DATE, SQS_UPDATE_DATE,"
      + " SQS_EXTERNAL_ID, SQS_EXTERNAL_LINK, SQS_EXCEPTION,"
      + " SQS_EXCEPTION_TRACE)"
      + "VALUES "
      + "(1, 'RUNNING', '2012-01-01 01:01:01', '2012-01-01 01:01:01', 'job_1',"
      +   "NULL, NULL, NULL),"
      + "(2, 'SUCCEEDED', '2012-01-01 01:01:01', '2012-01-02 01:01:01', 'job_2',"
      + " NULL, NULL, NULL),"
      + "(3, 'FAILED', '2012-01-01 01:01:01', '2012-01-03 01:01:01', 'job_3',"
      + " NULL, NULL, NULL),"
      + "(4, 'UNKNOWN', '2012-01-01 01:01:01', '2012-01-04 01:01:01', 'job_4',"
      + " NULL, NULL, NULL),"
      + "(1, 'RUNNING', '2012-01-01 01:01:01', '2012-01-05 01:01:01', 'job_5',"
      + " NULL, NULL, NULL)"
    );

    runQuery("INSERT INTO SQOOP.SQ_COUNTER_SUBMISSION "
      + "(SQRS_GROUP, SQRS_COUNTER, SQRS_SUBMISSION, SQRS_VALUE) "
      + "VALUES"
      + "(1, 1, 4, 300)"
    );

  }

  protected MConnector getConnector() {
    return new MConnector("A", "org.apache.sqoop.test.A", "1.0-test",
      getConnectionForms(), new MJobForms(getForms()), new MJobForms(getForms()));
  }

  protected MFramework getFramework() {
    return new MFramework(getConnectionForms(), new MJobForms(getForms()),
      FrameworkManager.CURRENT_FRAMEWORK_VERSION);
  }

  protected void fillConnection(MConnection connection) {
    List<MForm> forms;

    forms = connection.getConnectorPart().getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput)forms.get(1).getInputs().get(0)).setValue("Value2");

    forms = connection.getFrameworkPart().getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Value13");
    ((MStringInput)forms.get(1).getInputs().get(0)).setValue("Value15");
  }

  protected void fillJob(MJob job) {
    List<MForm> forms;

    forms = job.getConnectorPart(Direction.FROM).getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput)forms.get(1).getInputs().get(0)).setValue("Value2");

    forms = job.getConnectorPart(Direction.TO).getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput)forms.get(1).getInputs().get(0)).setValue("Value2");

    forms = job.getFrameworkPart().getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Value13");
    ((MStringInput)forms.get(1).getInputs().get(0)).setValue("Value15");
  }

  protected MConnectionForms getConnectionForms() {
    return new MConnectionForms(getForms());
  }

  protected List<MForm> getForms() {
    List<MForm> forms = new LinkedList<MForm>();

    List<MInput<?>> inputs;
    MInput input;

    inputs = new LinkedList<MInput<?>>();
    input = new MStringInput("I1", false, (short)30);
    inputs.add(input);
    input = new MMapInput("I2", false);
    inputs.add(input);
    forms.add(new MForm("F1", inputs));

    inputs = new LinkedList<MInput<?>>();
    input = new MStringInput("I3", false, (short)30);
    inputs.add(input);
    input = new MMapInput("I4", false);
    inputs.add(input);
    forms.add(new MForm("F2", inputs));

    return forms;
  }

  /**
   * Find out number of entries in given table.
   *
   * @param table Table name
   * @return Number of rows in the table
   * @throws Exception
   */
  protected long countForTable(String table) throws Exception {
    Statement stmt = null;
    ResultSet rs = null;

    try {
      stmt = getDerbyConnection().createStatement();

      rs = stmt.executeQuery("SELECT COUNT(*) FROM "+ table);
      rs.next();

      return rs.getLong(1);
    } finally {
      if(stmt != null) {
        stmt.close();
      }
      if(rs != null) {
        rs.close();
      }
    }
  }

  /**
   * Assert row count for given table.
   *
   * @param table Table name
   * @param expected Expected number of rows
   * @throws Exception
   */
  protected void assertCountForTable(String table, long expected)
    throws Exception {
    long count = countForTable(table);
    assertEquals(expected, count);
  }

  /**
   * Printout repository content for advance debugging.
   *
   * This method is currently unused, but might be helpful in the future, so
   * I'm letting it here.
   *
   * @throws Exception
   */
  protected void generateDatabaseState() throws Exception {
    for(String tbl : new String[] {"SQ_CONNECTOR", "SQ_FORM", "SQ_INPUT",
      "SQ_CONNECTION", "SQ_CONNECTION_INPUT", "SQ_JOB", "SQ_JOB_INPUT"}) {
      generateTableState("SQOOP." + tbl);
    }
  }

  /**
   * Printout one single table.
   *
   * @param table Table name
   * @throws Exception
   */
  protected void generateTableState(String table) throws Exception {
    PreparedStatement ps = null;
    ResultSet rs = null;
    ResultSetMetaData rsmt = null;

    try {
      ps = getDerbyConnection().prepareStatement("SELECT * FROM " + table);
      rs = ps.executeQuery();

      rsmt = rs.getMetaData();

      StringBuilder sb = new StringBuilder();
      System.out.println("Table " + table + ":");

      for(int i = 1; i <= rsmt.getColumnCount(); i++) {
        sb.append("| ").append(rsmt.getColumnName(i)).append(" ");
      }
      sb.append("|");
      System.out.println(sb.toString());

      while(rs.next()) {
        sb = new StringBuilder();
        for(int i = 1; i <= rsmt.getColumnCount(); i++) {
          sb.append("| ").append(rs.getString(i)).append(" ");
        }
        sb.append("|");
        System.out.println(sb.toString());
      }

      System.out.println("");

    } finally {
      if(rs != null) {
        rs.close();
      }
      if(ps != null) {
        ps.close();
      }
    }
  }
}
