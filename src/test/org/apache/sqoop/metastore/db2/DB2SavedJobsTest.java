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

package org.apache.sqoop.metastore.db2;

import org.apache.sqoop.metastore.SavedJobsTestBase;
import org.apache.sqoop.manager.JdbcDrivers;
import org.apache.sqoop.testcategories.thirdpartytest.Db2Test;
import org.junit.experimental.categories.Category;

/**
 * Test of GenericJobStorage compatibility with DB2
 *
 * This uses JDBC to store and retrieve metastore data from a DB2 server
 *
 * Since this requires a DB2 installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=DB2SavedJobsTest or -Dthirdparty=true.
 *
 * You need to put DB2 JDBC driver library (db2jcc4.jar) in a location
 * where Sqoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons) and set it's path through -Dsqoop.thirdparty.lib.dir.
 *
 *   Once you have a running DB2 database,
 *   Set server URL, database name, username, and password with system variables
 *   -Dsqoop.test.db2.connectstring.host_url, -Dsqoop.test.db2.connectstring.database,
 *   -Dsqoop.test.db2.connectstring.username and -Dsqoop.test.db2.connectstring.password respectively
 */
@Category(Db2Test.class)
public class DB2SavedJobsTest extends SavedJobsTestBase {

    private static final String HOST_URL = System.getProperty(
        "sqoop.test.db2.connectstring.host_url",
        "jdbc:db2://db2host:50000");

    private static final String DATABASE_NAME = System.getProperty(
        "sqoop.test.db2.connectstring.database",
        "SQOOP");
    private static final String DATABASE_USER = System.getProperty(
        "sqoop.test.db2.connectstring.username",
        "SQOOP");
    private static final String DATABASE_PASSWORD = System.getProperty(
        "sqoop.test.db2.connectstring.password",
        "SQOOP");
    private static final String CONNECT_STRING = HOST_URL
        + "/" + DATABASE_NAME
        + ":currentSchema=" + DATABASE_USER +";";

    public DB2SavedJobsTest() {
        super(CONNECT_STRING, DATABASE_USER, DATABASE_PASSWORD, JdbcDrivers.DB2.getDriverClass());
    }
}
