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

package org.apache.sqoop.metastore.postgres;

import org.apache.sqoop.metastore.SavedJobsTestBase;
import org.apache.sqoop.manager.JdbcDrivers;
import org.apache.sqoop.testcategories.thirdpartytest.PostgresqlTest;
import org.junit.experimental.categories.Category;

/**
 * Test of GenericJobStorage compatibility with PostgreSQL
 *
 * This uses JDBC to store and retrieve metastore data from a Postgres server
 *
 * Since this requires a Postgres installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=PostgresSavedJobsTest or -Dthirdparty=true.
 *
 *   Once you have a running Postgres database,
 *   Set server URL, database name, username, and password with system variables
 *   -Dsqoop.test.postgresql.connectstring.host_url, -Dsqoop.test.postgresql.database,
 *   -Dsqoop.test.postgresql.username and -Dsqoop.test.postgresql.password respectively
 */
@Category(PostgresqlTest.class)
public class PostgresSavedJobsTest extends SavedJobsTestBase {

    private static final String HOST_URL = System.getProperty("sqoop.test.postgresql.connectstring.host_url",
        "jdbc:postgresql://localhost/");
    private static final String DATABASE_USER = System.getProperty(
        "sqoop.test.postgresql.username", "sqooptest");
    private static final String DATABASE_NAME = System.getProperty(
        "sqoop.test.postgresql.database", "sqooptest");
    private static final String PASSWORD = System.getProperty("sqoop.test.postgresql.password");
    private static final String CONNECT_STRING = HOST_URL + DATABASE_NAME;

    public PostgresSavedJobsTest() {
        super(CONNECT_STRING, DATABASE_USER, PASSWORD, JdbcDrivers.POSTGRES.getDriverClass());
    }
}
