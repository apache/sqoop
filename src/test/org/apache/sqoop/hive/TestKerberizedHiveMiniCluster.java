package org.apache.sqoop.hive;

import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.hive.hiveserver2.HiveServer2ConnectionFactory;
import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.hive.minicluster.KerberosAuthenticationConfiguration;
import org.apache.sqoop.infrastructure.kerberos.MiniKdcInfrastructureRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class TestKerberizedHiveMiniCluster {

  private static final String CREATE_TABLE_SQL = "CREATE TABLE TestTable (id int)";

  private static final String INSERT_SQL = "INSERT INTO TestTable VALUES (?)";

  private static final String SELECT_SQL = "SELECT * FROM TestTable";

  private static final int TEST_VALUE = 10;

  private HiveMiniCluster hiveMiniCluster;

  private JdbcConnectionFactory connectionFactory;

  @ClassRule
  public static MiniKdcInfrastructureRule miniKdcInfrastructure = new MiniKdcInfrastructureRule();

  @Before
  public void before() {
    hiveMiniCluster = new HiveMiniCluster(new KerberosAuthenticationConfiguration(miniKdcInfrastructure));
    hiveMiniCluster.start();

    connectionFactory = new HiveServer2ConnectionFactory(hiveMiniCluster.getUrl(), null, null);
  }

  @Test
  public void testInsertedRowCanBeReadFromTable() throws Exception {
    createTestTable();
    insertRowIntoTestTable();

    assertEquals(TEST_VALUE, getDataFromTestTable());
  }

  private void insertRowIntoTestTable() throws SQLException {
    try (Connection conn = connectionFactory.createConnection(); PreparedStatement stmnt = conn.prepareStatement(INSERT_SQL)) {
      stmnt.setInt(1, TEST_VALUE);
      stmnt.executeUpdate();
    }
  }

  private int getDataFromTestTable() throws SQLException {
    try (Connection conn = connectionFactory.createConnection(); PreparedStatement stmnt = conn.prepareStatement(SELECT_SQL)) {
      ResultSet resultSet = stmnt.executeQuery();
      resultSet.next();
      return resultSet.getInt(1);
    }
  }

  private void createTestTable() throws SQLException {
    try (Connection conn = connectionFactory.createConnection(); PreparedStatement stmnt = conn.prepareStatement(CREATE_TABLE_SQL)) {
      stmnt.executeUpdate();
    }
  }

  @After
  public void after() {
    hiveMiniCluster.stop();
  }

}
