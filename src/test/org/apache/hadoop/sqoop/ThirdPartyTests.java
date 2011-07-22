/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package org.apache.hadoop.sqoop;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.sqoop.manager.DirectMySQLTest;
import org.apache.hadoop.sqoop.manager.DirectMySQLExportTest;
import org.apache.hadoop.sqoop.manager.JdbcMySQLExportTest;
import org.apache.hadoop.sqoop.manager.MySQLAuthTest;
import org.apache.hadoop.sqoop.manager.MySQLCompatTest;
import org.apache.hadoop.sqoop.manager.OracleExportTest;
import org.apache.hadoop.sqoop.manager.OracleManagerTest;
import org.apache.hadoop.sqoop.manager.OracleCompatTest;
import org.apache.hadoop.sqoop.manager.PostgresqlTest;

/**
 * Test battery including all tests of vendor-specific ConnManager
 * implementations.  These tests likely aren't run by Apache Hudson, because
 * they require configuring and using Oracle, MySQL, etc., which may have
 * incompatible licenses with Apache.
 */
public final class ThirdPartyTests extends TestCase {

  private ThirdPartyTests() { }

  public static Test suite() {
    TestSuite suite = new TestSuite("Tests vendor-specific ConnManager "
      + "implementations in Sqoop");
    suite.addTestSuite(DirectMySQLTest.class);
    suite.addTestSuite(DirectMySQLExportTest.class);
    suite.addTestSuite(JdbcMySQLExportTest.class);
    suite.addTestSuite(MySQLAuthTest.class);
    suite.addTestSuite(MySQLCompatTest.class);
    suite.addTestSuite(OracleExportTest.class);
    suite.addTestSuite(OracleManagerTest.class);
    suite.addTestSuite(OracleCompatTest.class);
    suite.addTestSuite(PostgresqlTest.class);

    return suite;
  }

}

