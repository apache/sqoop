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

package org.apache.sqoop.manager.oracle;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

public class OraOopTypesTest extends OraOopTestCase {
  @Test
  public void ensureTypesAfterExportMappedAsExpected() throws Exception {
    try {
      setSqoopTargetDirectory(getSqoopTargetDirectory() + "types_test");
      String tempTableName = "ORACLE_DATATYPES_TEMPLATE";
      String tableName = "ORACLE_DATATYPES";
      createTableFromSQL("create table " + tempTableName + " ("
          + "C1_NUM NUMBER(*,0),"
          + "C2_NUM NUMBER(*,5),"
          + "C3_NUM NUMBER(16,8),"
          + "C4_NUM NUMBER(9,-3),"
          + "C5_FLOAT BINARY_FLOAT,"
          + "C6_DOUBLE BINARY_DOUBLE,"
          + "C7_DATE DATE,"
          + "C8_TIMESTAMP TIMESTAMP,"
          + "C9_TIMESTAMP_WITH_TZ TIMESTAMP WITH TIME ZONE,"
          + "C10_TIMESTAMP_WITH_LTZ TIMESTAMP WITH LOCAL TIME ZONE,"
          + "C11_CHAR CHAR(255),"
          + "C12_VARCHAR VARCHAR(255),"
          + "C13_VARCHAR2 VARCHAR2(255),"
          + "C14_NCHAR NCHAR(255),"
          + "C15_NVARCHAR2 NVARCHAR2(255),"
          + "C16_URITYPE UriType"
          + ")", tempTableName);

      Connection conn = getTestEnvConnection();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("insert into " + tempTableName + " values ("
            + "123456789101112.123456789101112,"
            + "123456789101112.123456789101112,"
            + "12345678.12345678,"
            + "123456789101.123456789101112,"
            + "123456789101112.123456789101112,"
            + "123456789101112.123456789101112,"
            + "DATE '2015-02-23',"
            + "TIMESTAMP '2015-02-23 13:42:24.123456 -07:00',"
            + "TIMESTAMP '2015-02-23 13:42:24.123456 -08:00',"
            + "TIMESTAMP '2015-02-23 13:42:24.123456 -09:00',"
            + "'ÁRÍZTŰRŐTÜKÖRFÚRÓGÉP',"
            + "'ÁRÍZTŰRŐTÜKÖRFÚRÓGÉP',"
            + "'ÁRÍZTŰRŐTÜKÖRFÚRÓGÉP',"
            + "'ÁRÍZTŰRŐTÜKÖRFÚRÓGÉP',"
            + "'ÁRÍZTŰRŐTÜKÖRFÚRÓGÉP',"
            + "httpuritype.createuri('http://www.oracle.com'))");
      }
      conn.commit();
      runImport(tempTableName, getSqoopConf(), false);
      runExportFromTemplateTable(tempTableName, tableName, true);
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery(
            "select count(*) from ("
            + "select * from (select "
              + "T1.C1_NUM, "
              + "T1.C2_NUM, "
              + "T1.C3_NUM, "
              + "T1.C4_NUM, "
              + "T1.C5_FLOAT, "
              + "T1.C6_DOUBLE, "
              + "T1.C7_DATE, "
              + "T1.C8_TIMESTAMP, "
              + "T1.C9_TIMESTAMP_WITH_TZ, "
              + "T1.C10_TIMESTAMP_WITH_LTZ, "
              + "T1.C11_CHAR, "
              + "T1.C12_VARCHAR, "
              + "T1.C13_VARCHAR2, "
              + "T1.C14_NCHAR, "
              + "T1.C15_NVARCHAR2, "
              + "T1.C16_URITYPE.GETURL() from "
                + tempTableName
                + " T1 "
            + "minus select "
              + "T2.C1_NUM, "
              + "T2.C2_NUM, "
              + "T2.C3_NUM, "
              + "T2.C4_NUM, "
              + "T2.C5_FLOAT, "
              + "T2.C6_DOUBLE, "
              + "T2.C7_DATE, "
              + "T2.C8_TIMESTAMP, "
              + "T2.C9_TIMESTAMP_WITH_TZ, "
              + "T2.C10_TIMESTAMP_WITH_LTZ, "
              + "T2.C11_CHAR, "
              + "T2.C12_VARCHAR, "
              + "T2.C13_VARCHAR2, "
              + "T2.C14_NCHAR, "
              + "T2.C15_NVARCHAR2, "
              + "T2.C16_URITYPE.GETURL() from "+tableName+" T2) "
            + "union all select * from (select "
              + "T1.C1_NUM, "
              + "T1.C2_NUM, "
              + "T1.C3_NUM, "
              + "T1.C4_NUM, "
              + "T1.C5_FLOAT, "
              + "T1.C6_DOUBLE, "
              + "T1.C7_DATE, "
              + "T1.C8_TIMESTAMP, "
              + "T1.C9_TIMESTAMP_WITH_TZ, "
              + "T1.C10_TIMESTAMP_WITH_LTZ, "
              + "T1.C11_CHAR, "
              + "T1.C12_VARCHAR, "
              + "T1.C13_VARCHAR2, "
              + "T1.C14_NCHAR, "
              + "T1.C15_NVARCHAR2, "
              + "T1.C16_URITYPE.GETURL() from "+tableName+" T1 "
            + "minus select "
              + "T2.C1_NUM, "
              + "T2.C2_NUM, "
              + "T2.C3_NUM, "
              + "T2.C4_NUM, "
              + "T2.C5_FLOAT, "
              + "T2.C6_DOUBLE, "
              + "T2.C7_DATE, "
              + "T2.C8_TIMESTAMP, "
              + "T2.C9_TIMESTAMP_WITH_TZ, "
              + "T2.C10_TIMESTAMP_WITH_LTZ, "
              + "T2.C11_CHAR, "
              + "T2.C12_VARCHAR, "
              + "T2.C13_VARCHAR2, "
              + "T2.C14_NCHAR, "
              + "T2.C15_NVARCHAR2, "
              + "T2.C16_URITYPE.GETURL() from "+tempTableName+" T2))");
        rs.next();
        assertEquals(0, rs.getInt(1));
      }
    } finally {
      cleanupFolders();
      closeTestEnvConnection();
    }
  }
}
