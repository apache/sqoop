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

package org.apache.sqoop.metastore;

import org.apache.sqoop.testcategories.sqooptest.IntegrationTest;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.HsqldbTestServer;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.JobTool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class TestMetastoreConfigurationParameters {

    private static final int STATUS_FAILURE = 1;
    private static final int STATUS_SUCCESS = 0;
    private static final String TEST_USER = "sqoop";
    private static final String TEST_PASSWORD = "sqoop";
    private static final String DEFAULT_HSQLDB_USER = "SA";
    private static final String NON_DEFAULT_PASSWORD = "NOT_DEFAULT";
    private static final String DEFAULT_PASSWORD = "";
    private static HsqldbTestServer testHsqldbServer;

    private Sqoop sqoop;

    @BeforeClass
    public static void beforeClass() throws Exception {
        testHsqldbServer = new HsqldbTestServer();
        testHsqldbServer.start();
        setupUsersForTesting();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        testHsqldbServer.changePasswordForUser(DEFAULT_HSQLDB_USER, NON_DEFAULT_PASSWORD, DEFAULT_PASSWORD);
        testHsqldbServer.stop();
    }

    @Before
    public void before() {
        sqoop = new Sqoop(new JobTool());
    }

    @Test
    public void testJobToolWithAutoConnectDisabledFails() throws IOException {
        ArgumentArrayBuilder builder = new ArgumentArrayBuilder()
            .withProperty("sqoop.metastore.client.enable.autoconnect", "false");
        String[] arguments = builder.build();
        assertEquals(STATUS_FAILURE, Sqoop.runSqoop(sqoop, arguments));
    }

    @Test
    public void testJobToolWithAutoConnectUrlAndCorrectUsernamePasswordSpecifiedSuccessfullyRuns() {
        int status = runJobToolWithAutoConnectUrlAndCorrectUsernamePasswordSpecified();
        assertEquals(STATUS_SUCCESS, status);
    }

    @Test
    public void testJobToolWithAutoConnectUrlAndCorrectUsernamePasswordSpecifiedInitializesSpecifiedDatabase() throws SQLException {
        runJobToolWithAutoConnectUrlAndCorrectUsernamePasswordSpecified();
        verifyMetastoreIsInitialized();
    }

    private int runJobToolWithAutoConnectUrlAndCorrectUsernamePasswordSpecified() {
        ArgumentArrayBuilder builder = new ArgumentArrayBuilder()
            .withProperty("sqoop.metastore.client.autoconnect.url", HsqldbTestServer.getUrl())
            .withProperty("sqoop.metastore.client.autoconnect.username", TEST_USER)
            .withProperty("sqoop.metastore.client.autoconnect.password", TEST_PASSWORD)
            .withOption("list");
        String[] arguments = builder.build();
        return Sqoop.runSqoop(sqoop, arguments);
    }

    private static void setupUsersForTesting() throws SQLException {
        // We create a new user and change the password of SA to make sure that Sqoop does not connect to metastore with the default user and password.
        testHsqldbServer.createNewUser(TEST_USER, TEST_PASSWORD);
        testHsqldbServer.changePasswordForUser(DEFAULT_HSQLDB_USER, DEFAULT_PASSWORD, NON_DEFAULT_PASSWORD);
    }

    private void verifyMetastoreIsInitialized() throws SQLException {
        try (Connection connection = testHsqldbServer.getConnection(TEST_USER, TEST_PASSWORD); Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM SQOOP_ROOT");
            assertTrue(resultSet.next());
        }
    }

}
