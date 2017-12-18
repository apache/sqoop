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
package com.cloudera.sqoop.lib;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.io.BytesWritable;
import org.apache.sqoop.lib.BlobRef;

/**
 * Contains a set of methods which can read db columns from a ResultSet into
 * Java types, and do serialization of these types to/from DataInput/DataOutput
 * for use with Hadoop's Writable implementation. This supports null values
 * for all types.
 *
 * @deprecated use org.apache.sqoop.lib.JdbcWritableBridge instead.
 * @see org.apache.sqoop.lib.JdbcWritableBridge
 */
public final class JdbcWritableBridge {

  // Currently, cap BLOB/CLOB objects at 16 MB until we can use external
  // storage.
  public static final long MAX_BLOB_LENGTH =
      org.apache.sqoop.lib.JdbcWritableBridge.MAX_BLOB_LENGTH;
  public static final long MAX_CLOB_LENGTH =
      org.apache.sqoop.lib.JdbcWritableBridge.MAX_CLOB_LENGTH;

  private JdbcWritableBridge() {
  }

  public static Integer readInteger(int colNum, ResultSet r)
      throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readInteger(colNum, r);
  }

  public static Long readLong(int colNum, ResultSet r) throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readLong(colNum, r);
  }

  public static String readString(int colNum, ResultSet r) throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readString(colNum, r);
  }

  public static Float readFloat(int colNum, ResultSet r) throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readFloat(colNum, r);
  }

  public static Double readDouble(int colNum, ResultSet r) throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readDouble(colNum, r);
  }

  public static Boolean readBoolean(int colNum, ResultSet r)
     throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readBoolean(colNum, r);
  }

  public static Time readTime(int colNum, ResultSet r) throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readTime(colNum, r);
  }

  public static Timestamp readTimestamp(int colNum, ResultSet r)
      throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readTimestamp(colNum, r);
  }

  public static Date readDate(int colNum, ResultSet r) throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readDate(colNum, r);
  }

  public static BytesWritable readBytesWritable(int colNum, ResultSet r)
      throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readBytesWritable(colNum, r);
  }

  public static BigDecimal readBigDecimal(int colNum, ResultSet r)
      throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readBigDecimal(colNum, r);
  }

  public static BlobRef readBlobRef(int colNum, ResultSet r)
      throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readBlobRef(colNum, r);
  }

  public static ClobRef readClobRef(int colNum, ResultSet r)
      throws SQLException {
    return org.apache.sqoop.lib.JdbcWritableBridge.readClobRef(colNum, r);
  }

  public static void writeInteger(Integer val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeInteger(
        val, paramIdx, sqlType, s);
  }

  public static void writeLong(Long val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeLong(
        val, paramIdx, sqlType, s);
  }

  public static void writeDouble(Double val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeDouble(
        val, paramIdx, sqlType, s);
  }

  public static void writeBoolean(Boolean val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeBoolean(
        val, paramIdx, sqlType, s);
  }

  public static void writeFloat(Float val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeFloat(
        val, paramIdx, sqlType, s);
  }

  public static void writeString(String val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeString(
        val, paramIdx, sqlType, s);
  }

  public static void writeTimestamp(Timestamp val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeTimestamp(
        val, paramIdx, sqlType, s);
  }

  public static void writeTime(Time val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeTime(
        val, paramIdx, sqlType, s);
  }

  public static void writeDate(Date val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeDate(
        val, paramIdx, sqlType, s);
  }

  public static void writeBytesWritable(BytesWritable val, int paramIdx,
      int sqlType, PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeBytesWritable(
        val, paramIdx, sqlType, s);
  }

  public static void writeBigDecimal(BigDecimal val, int paramIdx,
      int sqlType, PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeBigDecimal(
        val, paramIdx, sqlType, s);
  }

  public static void writeBlobRef(BlobRef val, int paramIdx,
      int sqlType, PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeBlobRef(
        val, paramIdx, sqlType, s);
  }

  public static void writeClobRef(ClobRef val, int paramIdx,
      int sqlType, PreparedStatement s) throws SQLException {
    org.apache.sqoop.lib.JdbcWritableBridge.writeClobRef(
        val, paramIdx, sqlType, s);
  }
}
