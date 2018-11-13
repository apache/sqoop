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

package org.apache.sqoop.metastore.sqlserver;

import org.apache.sqoop.metastore.JobToolTestBase;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils;
import org.apache.sqoop.testcategories.thirdpartytest.SqlServerTest;
import org.junit.experimental.categories.Category;

/**
 * Test that the Job Tool works in SQLServer
 *
 * This uses JDBC to store and retrieve metastore data from an SQLServer
 *
 * Since this requires an SQLServer installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=SqlServerJobToolTest or -Dthirdparty=true.
 *
 * You need to put SQL Server JDBC driver library (sqljdbc4.jar) in a location
 * where Sqoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons) and set it's path through -Dsqoop.thirdparty.lib.dir.
 *
 *   Once you have a running SQLServer database,
 *   Set server URL, database name, username, and password with system variables
 *   -Dsqoop.test.sqlserver.connectstring.host_url, -Dsqoop.test.sqlserver.database,
 *   -Dms.sqlserver.username and -Dms.sqlserver.password respectively
 */
@Category(SqlServerTest.class)
public class SqlServerJobToolTest extends JobToolTestBase {

    private static MSSQLTestUtils msSQLTestUtils = new MSSQLTestUtils();

    public SqlServerJobToolTest() {
        super(msSQLTestUtils.getDBConnectString(),
                msSQLTestUtils.getDBUserName(),
                msSQLTestUtils.getDBPassWord());
    }
}
