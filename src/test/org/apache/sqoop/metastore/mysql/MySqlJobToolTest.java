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

package org.apache.sqoop.metastore.mysql;

import org.apache.sqoop.manager.mysql.MySQLTestUtils;
import org.apache.sqoop.metastore.JobToolTestBase;
import org.apache.sqoop.testcategories.thirdpartytest.MysqlTest;
import org.junit.experimental.categories.Category;

/**
 * Test that the Job Tool works in MySql
 *
 * This uses JDBC to store and retrieve metastore data from a MySql server
 *
 * Since this requires a MySql installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=MySqlJobToolTest or -Dthirdparty=true.
 *
 * You need to put MySql JDBC driver library (mysql-connector-java-5.1.38-bin.jar) in a location
 * where Sqoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons) and set it's path through -Dsqoop.thirdparty.lib.dir.
 *
 *   Once you have a running MySql database,
 *   Set server URL, database name, username, and password with system variables
 *   -Dsqoop.test.mysql.connectstring.host_url, -Dsqoop.test.mysql.databasename,
 *   -Dsqoop.test.mysql.username and -Dsqoop.test.mysql.password respectively
 */
@Category(MysqlTest.class)
public class MySqlJobToolTest extends JobToolTestBase {

    private static MySQLTestUtils mySQLTestUtils = new MySQLTestUtils();

    public MySqlJobToolTest() {
        super(mySQLTestUtils.getMySqlConnectString(), mySQLTestUtils.getUserName(),
                mySQLTestUtils.getUserPass());
    }
}
