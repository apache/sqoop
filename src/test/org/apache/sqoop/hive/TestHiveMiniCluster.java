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

package org.apache.sqoop.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.hive.minicluster.AuthenticationConfiguration;
import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.hive.minicluster.KerberosAuthenticationConfiguration;
import org.apache.sqoop.hive.minicluster.NoAuthenticationConfiguration;
import org.apache.sqoop.hive.minicluster.PasswordAuthenticationConfiguration;
import org.apache.sqoop.infrastructure.kerberos.MiniKdcInfrastructureRule;
import org.apache.sqoop.testcategories.sqooptest.IntegrationTest;
import org.apache.sqoop.testcategories.KerberizedTest;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@Category({KerberizedTest.class, IntegrationTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
public class TestHiveMiniCluster {

  @ClassRule
  public static MiniKdcInfrastructureRule miniKdcInfrastructure = new MiniKdcInfrastructureRule();

  private static final String TEST_USERNAME = "sqoop";

  private static final String TEST_PASSWORD = "secret";

  @Parameters(name = "config = {0}")
  public static Iterable<? extends Object> authenticationParameters() {
    return Arrays.asList(new NoAuthenticationConfiguration(),
                         new PasswordAuthenticationConfiguration(TEST_USERNAME, TEST_PASSWORD),
                         new KerberosAuthenticationConfiguration(miniKdcInfrastructure));
  }

  private static final String CREATE_TABLE_SQL = "CREATE TABLE TestTable (id int)";

  private static final String INSERT_SQL = "INSERT INTO TestTable VALUES (?)";

  private static final String SELECT_SQL = "SELECT * FROM TestTable";

  private static final int TEST_VALUE = 10;

  private final AuthenticationConfiguration authenticationConfiguration;

  private HiveMiniCluster hiveMiniCluster;

  private JdbcConnectionFactory connectionFactory;

  public TestHiveMiniCluster(AuthenticationConfiguration authenticationConfiguration) {
    this.authenticationConfiguration = authenticationConfiguration;
  }

  @Before
  public void before() throws SQLException {
    hiveMiniCluster = new HiveMiniCluster(authenticationConfiguration);
    hiveMiniCluster.start();

    connectionFactory = authenticationConfiguration.decorateConnectionFactory(new HiveServer2ConnectionFactory(hiveMiniCluster.getUrl(), TEST_USERNAME, TEST_PASSWORD));
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
    UserGroupInformation.setConfiguration(new Configuration());
  }

}
