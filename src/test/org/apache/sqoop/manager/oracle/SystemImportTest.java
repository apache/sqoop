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

import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.sqoop.manager.oracle.util.*;
import org.junit.Test;

import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.manager.OracleUtils;

/**
 * OraOop system tests of importing data from oracle to hadoop.
 */
public class SystemImportTest extends OraOopTestCase {

  private static Class<?> preparedStatementClass;
  private static Method methSetBinaryDouble;
  private static Method methSetBinaryFloat;

  static {
    try {
      preparedStatementClass =
          Class.forName("oracle.jdbc.OraclePreparedStatement");
      methSetBinaryDouble =
          preparedStatementClass.getMethod("setBinaryDouble", int.class,
              double.class);
      methSetBinaryFloat =
          preparedStatementClass.getMethod("setBinaryFloat", int.class,
              float.class);
    } catch (Exception e) {
      throw new RuntimeException(
          "Problem getting Oracle JDBC methods via reflection.", e);
    }
  }

  /**
   * Generates pseudo-random test data across all supported data types in an
   * Oracle database. Imports the data into Hadoop and compares with the data in
   * Oracle.
   *
   * @throws Exception
   */
  @Test
  public void importTest() throws Exception {
    // Generate test data in oracle
    setSqoopTargetDirectory(getSqoopTargetDirectory()
        + OracleUtils.SYSTEMTEST_TABLE_NAME);
    int numRows = OracleUtils.SYSTEMTEST_NUM_ROWS;
    Connection conn = getTestEnvConnection();
    OraOopOracleQueries.setConnectionTimeZone(conn, "GMT");
    try {
      Statement s = conn.createStatement();
      try {
        s.executeUpdate("CREATE TABLE "
            + OracleUtils.SYSTEMTEST_TABLE_NAME
            + " (id NUMBER(10) PRIMARY KEY, bd BINARY_DOUBLE, bf BINARY_FLOAT, "
            + "b BLOB, c CHAR(12), cl CLOB, d DATE, "
            + "f FLOAT(126), l LONG, nc NCHAR(30), ncl NCLOB, n NUMBER(9,2), "
            + "nvc NVARCHAR2(30), r ROWID, u URITYPE, iym INTERVAL YEAR(2) TO "
            + "MONTH, ids INTERVAL DAY(2) TO SECOND(6), "
            + "t TIMESTAMP(6), tz TIMESTAMP(6) WITH TIME ZONE, "
            + "tltz TIMESTAMP(6) WITH LOCAL TIME ZONE, rawcol RAW(21))");
        BinaryDoubleGenerator bdg = new BinaryDoubleGenerator();
        BinaryFloatGenerator bfg = new BinaryFloatGenerator();
        BlobGenerator bg = new BlobGenerator(conn, 2 * 1024, 8 * 1024);
        CharGenerator cg = new CharGenerator(12, 12);
        CharGenerator clobg = new CharGenerator(2 * 1024, 8 * 1024);
        TimestampGenerator dateg = new TimestampGenerator(0);
        FloatGenerator fg = new FloatGenerator(126);
        CharGenerator lg = new CharGenerator(2 * 1024, 8 * 1024);
        NCharGenerator ncg = new NCharGenerator(30, 30);
        NCharGenerator nclobg = new NCharGenerator(2 * 1024, 8 * 1024);
        BigDecimalGenerator ng = new BigDecimalGenerator(9, 2);
        NCharGenerator nvcg = new NCharGenerator(1, 30);
        RowIdGenerator rg = new RowIdGenerator();
        URIGenerator ug = new URIGenerator();
        IntervalYearMonthGenerator iymg = new IntervalYearMonthGenerator(2);
        IntervalDaySecondGenerator idsg = new IntervalDaySecondGenerator(2, 6);
        TimestampGenerator tg = new TimestampGenerator(6);
        TimestampGenerator tzg = new TimestampGenerator(6);
        TimestampGenerator tltzg = new TimestampGenerator(6);
        BytesGenerator rawg = new BytesGenerator(21, 21);
        PreparedStatement ps =
            conn.prepareStatement("INSERT INTO "
              + OracleUtils.SYSTEMTEST_TABLE_NAME
              + " ( id, bd, bf, b, c, cl, d, f, nc, ncl, n, nvc, r, u, iym, "
              + "ids, t, tz, tltz, rawcol ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, sys.UriFactory.getUri(?), ?, ?, ?, ?, ?, ? )");
        try {
          for (int i = 0; i < numRows; i++) {
            ps.setInt(1, i);
            methSetBinaryDouble.invoke(ps, 2, bdg.next());
            methSetBinaryFloat.invoke(ps, 3, bfg.next());
            ps.setBlob(4, bg.next());
            ps.setString(5, cg.next());
            ps.setString(6, clobg.next());
            ps.setTimestamp(7, dateg.next());
            ps.setBigDecimal(8, fg.next());
            ps.setString(9, ncg.next());
            ps.setString(10, nclobg.next());
            ps.setBigDecimal(11, ng.next());
            ps.setString(12, nvcg.next());
            ps.setRowId(13, rg.next());
            ps.setString(14, ug.next());
            ps.setString(15, iymg.next());
            ps.setString(16, idsg.next());
            ps.setTimestamp(17, tg.next());
            ps.setTimestamp(18, tzg.next());
            ps.setTimestamp(19, tltzg.next());
            ps.setBytes(20, rawg.next());
            ps.executeUpdate();
          }
        } finally {
          ps.close();
          conn.commit();
        }

        // Can't bind > 4000 bytes of data to LONG and LOB columns in the same
        // statement, so do LONG by itself
        ps =
            conn.prepareStatement("UPDATE " + OracleUtils.SYSTEMTEST_TABLE_NAME
                + " SET l = ? WHERE id = ?");
        try {
          for (int i = 0; i < numRows; i++) {
            ps.setString(1, lg.next());
            ps.setInt(2, i);
            ps.executeUpdate();
          }
        } finally {
          ps.close();
          conn.commit();
        }

        try {
          // Import test data into hadoop

          int retCode =
            runImport(OracleUtils.SYSTEMTEST_TABLE_NAME, getSqoopConf(), true);
          assertEquals("Return code should be 0", 0, retCode);

          // Add sqoop generated code to the classpath
          String sqoopGenJarPath =
              "file://" + getSqoopGenLibDirectory() + "/"
                  + getSqoopGenClassName() + ".jar";
          URLClassLoader loader =
              new URLClassLoader(new URL[] { new URL(sqoopGenJarPath) },
                  getClass().getClassLoader());
          Thread.currentThread().setContextClassLoader(loader);

          // Read test data from hadoop
          Configuration hadoopConf = getSqoopConf();
          FileSystem hdfs = FileSystem.get(hadoopConf);
          Path path = new Path(getSqoopTargetDirectory());
          FileStatus[] statuses = hdfs.listStatus(path);
          int hadoopRecordCount = 0;
          for (FileStatus status : statuses) {
            if (status.getPath().getName().startsWith("part-m-")) {

              SequenceFile.Reader reader =
                  new SequenceFile.Reader(hdfs, status.getPath(), hadoopConf);
              LongWritable key = new LongWritable();
              @SuppressWarnings("unchecked")
              SqoopRecord value =
                  ((Class<SqoopRecord>) reader.getValueClass())
                      .getConstructor().newInstance();
              ps =
                  conn.prepareStatement("SELECT bd, bf, b, c, cl, d, f, l, nc, "
                      + "ncl, nvc, r, u, iym, ids, t, tz, tltz, rawcol FROM "
                      + OracleUtils.SYSTEMTEST_TABLE_NAME + " WHERE id = ?");
              while (reader.next(key, value)) {
                // Compare test data from hadoop with data in oracle
                Map<String, Object> fields = value.getFieldMap();
                BigDecimal id = (BigDecimal) fields.get("ID");
                ps.setBigDecimal(1, id);
                ResultSet rs = ps.executeQuery();
                assertTrue("Did not find row with id " + id + " in oracle", rs
                    .next());
                assertEquals("BinaryDouble did not match for row " + id, fields
                    .get("BD"), rs.getDouble(1));
                assertEquals("BinaryFloat did not match for row " + id, fields
                    .get("BF"), rs.getFloat(2));
                // LONG column needs to be read before BLOB column
                assertEquals("Long did not match for row " + id, fields
                    .get("L"), rs.getString(8));
                BlobRef hadoopBlob = (BlobRef) fields.get("B");
                Blob oraBlob = rs.getBlob(3);
                assertTrue("Blob did not match for row " + id, Arrays.equals(
                    hadoopBlob.getData(), oraBlob.getBytes(1L, (int) oraBlob
                        .length())));
                assertEquals("Char did not match for row " + id, fields
                    .get("C"), rs.getString(4));
                ClobRef hadoopClob = (ClobRef) fields.get("CL");
                Clob oraClob = rs.getClob(5);
                assertEquals("Clob did not match for row " + id, hadoopClob
                  .getData(), oraClob.getSubString(1, (int) oraClob.length()));
                assertEquals("Date did not match for row " + id, fields
                    .get("D"), rs.getString(6));
                BigDecimal hadoopFloat = (BigDecimal) fields.get("F");
                BigDecimal oraFloat = rs.getBigDecimal(7);
                assertEquals("Float did not match for row " + id, hadoopFloat,
                    oraFloat);
                assertEquals("NChar did not match for row " + id, fields
                    .get("NC"), rs.getString(9));
                assertEquals("NClob did not match for row " + id, fields
                    .get("NCL"), rs.getString(10));
                assertEquals("NVarChar did not match for row " + id, fields
                    .get("NVC"), rs.getString(11));
                assertEquals("RowId did not match for row " + id, fields
                    .get("R"), new String(rs.getRowId(12).getBytes()));
                Struct url = (Struct) rs.getObject(13); // TODO: Find a fix for
                                                        // this workaround
                String urlString = (String) url.getAttributes()[0];
                if (url.getSQLTypeName().equals("SYS.HTTPURITYPE")) {
                  urlString = "http://" + urlString;
                } else if (url.getSQLTypeName().equals("SYS.DBURITYPE")) {
                  urlString = "/ORADB" + urlString;
                }
                assertEquals("UriType did not match for row " + id, fields
                    .get("U"), urlString);
                assertEquals("Interval Year to Month did not match for row "
                    + id, fields.get("IYM"), rs.getString(14));
                String ids = (String) fields.get("IDS"); // Strip trailing zeros
                                                         // to match oracle
                                                         // format
                int lastNonZero = ids.length() - 1;
                while (ids.charAt(lastNonZero) == '0') {
                  lastNonZero--;
                }
                ids = ids.substring(0, lastNonZero + 1);
                assertEquals("Interval Day to Second did not match for row "
                    + id, ids, rs.getString(15));
                assertEquals("Timestamp did not match for row " + id, fields
                    .get("T"), rs.getString(16));
                assertEquals("Timestamp with Time Zone did not match for row "
                    + id, fields.get("TZ"), rs.getString(17));
                assertEquals(
                    "Timestamp with Local Time Zone did not match for row "
                        + id, fields.get("TLTZ"), rs.getString(18));
                BytesWritable rawCol = (BytesWritable) fields.get("RAWCOL");
                byte[] rawColData =
                    Arrays.copyOf(rawCol.getBytes(), rawCol.getLength());
                assertTrue("RAW did not match for row " + id, Arrays.equals(
                    rawColData, rs.getBytes(19)));

                assertFalse("Found multiple rows with id " + id + " in oracle",
                    rs.next());
                hadoopRecordCount++;
              }
              reader.close();
            }
          }
          ResultSet rs =
              s.executeQuery("SELECT COUNT(*) FROM "
                  + OracleUtils.SYSTEMTEST_TABLE_NAME);
          rs.next();
          int oracleRecordCount = rs.getInt(1);
          assertEquals(
              "Number of records in Hadoop does not match number of "
              + "records in oracle",
              hadoopRecordCount, oracleRecordCount);
          rs.close();
        } finally {
          // Delete test data from hadoop
          cleanupFolders();
        }
      } finally {
        // Delete test data from oracle
        s.executeUpdate("DROP TABLE " + OracleUtils.SYSTEMTEST_TABLE_NAME);
        s.close();
      }

    } finally {
      closeTestEnvConnection();
    }
  }
}
