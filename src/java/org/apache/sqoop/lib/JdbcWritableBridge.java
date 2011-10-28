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
package org.apache.sqoop.lib;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.io.BytesWritable;

import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;

/**
 * Contains a set of methods which can read db columns from a ResultSet into
 * Java types, and do serialization of these types to/from DataInput/DataOutput
 * for use with Hadoop's Writable implementation. This supports null values
 * for all types.
 */
public final class JdbcWritableBridge {

  // Currently, cap BLOB/CLOB objects at 16 MB until we can use external
  // storage.
  public static final long MAX_BLOB_LENGTH = 16 * 1024 * 1024;
  public static final long MAX_CLOB_LENGTH = 16 * 1024 * 1024;

  private JdbcWritableBridge() {
  }

  public static Integer readInteger(int colNum, ResultSet r)
      throws SQLException {
    int val;
    val = r.getInt(colNum);
    if (r.wasNull()) {
      return null;
    } else {
      return Integer.valueOf(val);
    }
  }

  public static Long readLong(int colNum, ResultSet r) throws SQLException {
    long val;
    val = r.getLong(colNum);
    if (r.wasNull()) {
      return null;
    } else {
      return Long.valueOf(val);
    }
  }

  public static String readString(int colNum, ResultSet r) throws SQLException {
    return r.getString(colNum);
  }

  public static Float readFloat(int colNum, ResultSet r) throws SQLException {
    float val;
    val = r.getFloat(colNum);
    if (r.wasNull()) {
      return null;
    } else {
      return Float.valueOf(val);
    }
  }

  public static Double readDouble(int colNum, ResultSet r) throws SQLException {
    double val;
    val = r.getDouble(colNum);
    if (r.wasNull()) {
      return null;
    } else {
      return Double.valueOf(val);
    }
  }

  public static Boolean readBoolean(int colNum, ResultSet r)
      throws SQLException {
    boolean val;
    val = r.getBoolean(colNum);
    if (r.wasNull()) {
      return null;
    } else {
      return Boolean.valueOf(val);
    }
  }

  public static Time readTime(int colNum, ResultSet r) throws SQLException {
    return r.getTime(colNum);
  }

  public static Timestamp readTimestamp(int colNum, ResultSet r)
      throws SQLException {
    return r.getTimestamp(colNum);
  }

  public static Date readDate(int colNum, ResultSet r) throws SQLException {
    return r.getDate(colNum);
  }

  public static BytesWritable readBytesWritable(int colNum, ResultSet r)
      throws SQLException {
    byte [] bytes = r.getBytes(colNum);
    return bytes == null ? null : new BytesWritable(bytes);
  }

  public static BigDecimal readBigDecimal(int colNum, ResultSet r)
      throws SQLException {
    return r.getBigDecimal(colNum);
  }

  public static BlobRef readBlobRef(int colNum, ResultSet r)
      throws SQLException {
    // Loading of BLOBs is delayed; handled by LargeObjectLoader.
    return null;
  }

  public static ClobRef readClobRef(int colNum, ResultSet r)
      throws SQLException {
    // Loading of CLOBs is delayed; handled by LargeObjectLoader.
    return null;
  }

  public static void writeInteger(Integer val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    if (null == val) {
      s.setNull(paramIdx, sqlType);
    } else {
      s.setInt(paramIdx, val);
    }
  }

  public static void writeLong(Long val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    if (null == val) {
      s.setNull(paramIdx, sqlType);
    } else {
      s.setLong(paramIdx, val);
    }
  }

  public static void writeDouble(Double val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    if (null == val) {
      s.setNull(paramIdx, sqlType);
    } else {
      s.setDouble(paramIdx, val);
    }
  }

  public static void writeBoolean(Boolean val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    if (null == val) {
      s.setNull(paramIdx, sqlType);
    } else {
      s.setBoolean(paramIdx, val);
    }
  }

  public static void writeFloat(Float val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    if (null == val) {
      s.setNull(paramIdx, sqlType);
    } else {
      s.setFloat(paramIdx, val);
    }
  }

  public static void writeString(String val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    if (null == val) {
      s.setNull(paramIdx, sqlType);
    } else {
      s.setString(paramIdx, val);
    }
  }

  public static void writeTimestamp(Timestamp val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    if (null == val) {
      s.setNull(paramIdx, sqlType);
    } else {
      s.setTimestamp(paramIdx, val);
    }
  }

  public static void writeTime(Time val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    if (null == val) {
      s.setNull(paramIdx, sqlType);
    } else {
      s.setTime(paramIdx, val);
    }
  }

  public static void writeDate(Date val, int paramIdx, int sqlType,
      PreparedStatement s) throws SQLException {
    if (null == val) {
      s.setNull(paramIdx, sqlType);
    } else {
      s.setDate(paramIdx, val);
    }
  }

  public static void writeBytesWritable(BytesWritable val, int paramIdx,
      int sqlType, PreparedStatement s) throws SQLException {
    if (null == val) {
      s.setNull(paramIdx, sqlType);
    } else {
      // val.getBytes() is only valid in [0, len)
      byte [] rawBytes = val.getBytes();
      int len = val.getLength();
      byte [] outBytes = new byte[len];
      System.arraycopy(rawBytes, 0, outBytes, 0, len);
      s.setBytes(paramIdx, outBytes);
    }
  }

  public static void writeBigDecimal(BigDecimal val, int paramIdx,
      int sqlType, PreparedStatement s) throws SQLException {
    if (null == val) {
      s.setNull(paramIdx, sqlType);
    } else {
      s.setBigDecimal(paramIdx, val);
    }
  }

  public static void writeBlobRef(com.cloudera.sqoop.lib.BlobRef val,
      int paramIdx, int sqlType, PreparedStatement s) throws SQLException {
    // TODO: support this.
    throw new RuntimeException("Unsupported: Cannot export BLOB data");
  }

  public static void writeClobRef(com.cloudera.sqoop.lib.ClobRef val,
      int paramIdx, int sqlType, PreparedStatement s) throws SQLException {
    // TODO: support this.
    throw new RuntimeException("Unsupported: Cannot export CLOB data");
  }
}
