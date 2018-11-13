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

package org.apache.sqoop.metastore.oracle;

import org.apache.sqoop.manager.oracle.util.OracleUtils;
import org.apache.sqoop.metastore.SavedJobsTestBase;
import org.apache.sqoop.manager.JdbcDrivers;
import org.apache.sqoop.testcategories.thirdpartytest.OracleTest;
import org.junit.experimental.categories.Category;

/**
 * Test of GenericJobStorage compatibility with Oracle
 *
 * This uses JDBC to store and retrieve metastore data from an Oracle server
 *
 * Since this requires an Oracle installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=OracleSavedJobsTest or -Dthirdparty=true.
 *
 * You need to put Oracle JDBC driver library (ojdbc6.jar) in a location
 * where Sqoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons) and set it's path through -Dsqoop.thirdparty.lib.dir.
 *
 *   Once you have a running Oracle database,
 *   Set server URL, username, and password with system variables
 *   -Dsqoop.test.oracle.connectstring, -Dsqoop.test.oracle.username
 *   and -Dsqoop.test.oracle.password respectively
 */
@Category(OracleTest.class)
public class OracleSavedJobsTest extends SavedJobsTestBase {

    public OracleSavedJobsTest() {
        super(OracleUtils.CONNECT_STRING,
                OracleUtils.ORACLE_USER_NAME,
                OracleUtils.ORACLE_USER_PASS,
                JdbcDrivers.ORACLE.getDriverClass());
    }
}
