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

package org.apache.sqoop.db;

import org.apache.sqoop.testcategories.sqooptest.IntegrationTest;
import org.apache.sqoop.testutil.HsqldbTestServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.sql.Connection;

import static org.junit.Assert.assertFalse;

@Category(IntegrationTest.class)
public class TestDriverManagerJdbcConnectionFactory {

  private static final String HSQLDB_DRIVER_CLASS = "org.hsqldb.jdbcDriver";

  private static final String POSTGRESQL_DRIVER_CLASS = "org.postgresql.Driver";

  private static final String DB_USERNAME = "testuser";

  private static final String DB_PASSWORD = "testpassword";

  private static HsqldbTestServer hsqldbTestServer;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private DriverManagerJdbcConnectionFactory connectionFactory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    hsqldbTestServer = new HsqldbTestServer();
    hsqldbTestServer.start();

    hsqldbTestServer.createNewUser(DB_USERNAME, DB_PASSWORD);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    hsqldbTestServer.stop();
  }

  @Test
  public void testCreateConnectionThrowsWithInvalidDriverClass() throws Exception {
    String invalidDriverClass = "this_is_an_invalid_driver_class";
    connectionFactory = new DriverManagerJdbcConnectionFactory(invalidDriverClass, HsqldbTestServer.getUrl(), DB_USERNAME, DB_PASSWORD);

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Could not load db driver class: this_is_an_invalid_driver_class");
    connectionFactory.createConnection();
  }

  @Test
  public void testCreateConnectionThrowsWithoutRunningDatabase() throws Exception {
    String notRunningDb = "jdbc:postgresql://myhost:1234/database";
    connectionFactory = new DriverManagerJdbcConnectionFactory(POSTGRESQL_DRIVER_CLASS, notRunningDb, DB_USERNAME, DB_PASSWORD);

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Establishing connection failed!");
    connectionFactory.createConnection();
  }

  @Test
  public void testCreateConnectionThrowsWithInvalidUsername() throws Exception {
    String invalidUsername = "invalid_username";
    connectionFactory = new DriverManagerJdbcConnectionFactory(HSQLDB_DRIVER_CLASS, HsqldbTestServer.getUrl(), invalidUsername, DB_PASSWORD);

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Establishing connection failed!");
    connectionFactory.createConnection();
  }

  @Test
  public void testCreateConnectionThrowsWithInvalidPassword() throws Exception {
    String invalidPassword = "invalid_password";
    connectionFactory = new DriverManagerJdbcConnectionFactory(HSQLDB_DRIVER_CLASS, HsqldbTestServer.getUrl(), DB_USERNAME, invalidPassword);

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Establishing connection failed!");
    connectionFactory.createConnection();
  }

  @Test
  public void testCreateConnectionSucceedsWithValidParameters() throws Exception {
    connectionFactory = new DriverManagerJdbcConnectionFactory(HSQLDB_DRIVER_CLASS, HsqldbTestServer.getUrl(), DB_USERNAME, DB_PASSWORD);

    try (Connection connection = connectionFactory.createConnection()) {
      assertFalse(connection.isClosed());
    }
  }

}
