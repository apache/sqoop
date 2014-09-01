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

package com.cloudera.sqoop;

import com.cloudera.sqoop.hbase.HBaseImportAddRowKeyTest;
import com.cloudera.sqoop.hbase.HBaseImportNullTest;
import com.cloudera.sqoop.hbase.HBaseImportTypesTest;
import com.cloudera.sqoop.manager.DB2ManagerImportManualTest;

import org.apache.sqoop.hcat.HCatalogExportTest;
import org.apache.sqoop.hcat.HCatalogImportTest;

import com.cloudera.sqoop.hbase.HBaseImportTest;
import com.cloudera.sqoop.hbase.HBaseQueryImportTest;
import com.cloudera.sqoop.hbase.HBaseUtilTest;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.cloudera.sqoop.manager.CubridManagerExportTest;
import com.cloudera.sqoop.manager.CubridManagerImportTest;
import com.cloudera.sqoop.manager.DirectMySQLTest;
import com.cloudera.sqoop.manager.DirectMySQLExportTest;
import com.cloudera.sqoop.manager.JdbcMySQLExportTest;
import com.cloudera.sqoop.manager.MySQLAuthTest;
import com.cloudera.sqoop.manager.MySQLCompatTest;
import com.cloudera.sqoop.manager.OracleExportTest;
import com.cloudera.sqoop.manager.OracleManagerTest;
import com.cloudera.sqoop.manager.OracleCompatTest;
import com.cloudera.sqoop.manager.PostgresqlExportTest;
import com.cloudera.sqoop.manager.PostgresqlImportTest;

import org.apache.sqoop.manager.cubrid.CubridAuthTest;
import org.apache.sqoop.manager.cubrid.CubridCompatTest;
import org.apache.sqoop.manager.mysql.MySqlCallExportTest;
import org.apache.sqoop.manager.netezza.DirectNetezzaExportManualTest;
import org.apache.sqoop.manager.netezza.DirectNetezzaHCatExportManualTest;
import org.apache.sqoop.manager.netezza.DirectNetezzaHCatImportManualTest;
import org.apache.sqoop.manager.netezza.NetezzaExportManualTest;
import org.apache.sqoop.manager.netezza.NetezzaImportManualTest;
import org.apache.sqoop.manager.oracle.OracleCallExportTest;
import org.apache.sqoop.manager.oracle.OracleIncrementalImportTest;
import org.apache.sqoop.manager.sqlserver.SQLServerDatatypeExportDelimitedFileManualTest;
import org.apache.sqoop.manager.sqlserver.SQLServerDatatypeExportSequenceFileManualTest;
import org.apache.sqoop.manager.sqlserver.SQLServerDatatypeImportDelimitedFileManualTest;
import org.apache.sqoop.manager.sqlserver.SQLServerDatatypeImportSequenceFileManualTest;
import org.apache.sqoop.manager.sqlserver.SQLServerHiveImportManualTest;
import org.apache.sqoop.manager.sqlserver.SQLServerManagerManualTest;
import org.apache.sqoop.manager.sqlserver.SQLServerMultiColsManualTest;
import org.apache.sqoop.manager.sqlserver.SQLServerMultiMapsManualTest;
import org.apache.sqoop.manager.sqlserver.SQLServerParseMethodsManualTest;
import org.apache.sqoop.manager.sqlserver.SQLServerQueryManualTest;
import org.apache.sqoop.manager.sqlserver.SQLServerSplitByManualTest;
import org.apache.sqoop.manager.sqlserver.SQLServerWhereManualTest;

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
      + "implementations in Sqoop and tests with third party dependencies");

    // MySQL
    suite.addTestSuite(DirectMySQLTest.class);
    suite.addTestSuite(DirectMySQLExportTest.class);
    suite.addTestSuite(JdbcMySQLExportTest.class);
    suite.addTestSuite(MySQLAuthTest.class);
    suite.addTestSuite(MySQLCompatTest.class);

    // Oracle
    suite.addTestSuite(OracleExportTest.class);
    suite.addTestSuite(OracleManagerTest.class);
    suite.addTestSuite(OracleCompatTest.class);
    suite.addTestSuite(OracleIncrementalImportTest.class);

    // SQL Server
    suite.addTestSuite(SQLServerDatatypeExportDelimitedFileManualTest.class);
    suite.addTestSuite(SQLServerDatatypeExportSequenceFileManualTest.class);
    suite.addTestSuite(SQLServerDatatypeImportDelimitedFileManualTest.class);
    suite.addTestSuite(SQLServerDatatypeImportSequenceFileManualTest.class);
    suite.addTestSuite(SQLServerHiveImportManualTest.class);
    suite.addTestSuite(SQLServerManagerManualTest.class);
    suite.addTestSuite(SQLServerMultiColsManualTest.class);
    suite.addTestSuite(SQLServerMultiMapsManualTest.class);
    suite.addTestSuite(SQLServerParseMethodsManualTest.class);
    suite.addTestSuite(SQLServerQueryManualTest.class);
    suite.addTestSuite(SQLServerSplitByManualTest.class);
    suite.addTestSuite(SQLServerWhereManualTest.class);

    // PostgreSQL
    suite.addTestSuite(PostgresqlImportTest.class);
    suite.addTestSuite(PostgresqlExportTest.class);

    // Cubrid
    suite.addTestSuite(CubridManagerImportTest.class);
    suite.addTestSuite(CubridManagerExportTest.class);
    suite.addTestSuite(CubridAuthTest.class);
    suite.addTestSuite(CubridCompatTest.class);

    // DB2
    suite.addTestSuite(DB2ManagerImportManualTest.class);

    // Hbase
    suite.addTestSuite(HBaseImportTest.class);
    suite.addTestSuite(HBaseImportAddRowKeyTest.class);
    suite.addTestSuite(HBaseImportNullTest.class);
    suite.addTestSuite(HBaseImportTypesTest.class);
    suite.addTestSuite(HBaseQueryImportTest.class);
    suite.addTestSuite(HBaseUtilTest.class);

    // HCatalog
    suite.addTestSuite(HCatalogImportTest.class);
    suite.addTestSuite(HCatalogExportTest.class);

    // Call Export tests
    suite.addTestSuite(MySqlCallExportTest.class);
    suite.addTestSuite(OracleCallExportTest.class);

    // Netezza
    suite.addTestSuite(NetezzaExportManualTest.class);
    suite.addTestSuite(NetezzaImportManualTest.class);
    suite.addTestSuite(DirectNetezzaExportManualTest.class);
    suite.addTestSuite(DirectNetezzaHCatExportManualTest.class);
    suite.addTestSuite(DirectNetezzaHCatImportManualTest.class);

    return suite;
  }

}

