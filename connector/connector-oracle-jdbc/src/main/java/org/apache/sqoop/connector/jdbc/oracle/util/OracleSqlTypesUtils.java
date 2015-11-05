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
package org.apache.sqoop.connector.jdbc.oracle.util;

import java.sql.Types;

import org.apache.log4j.Logger;
import org.apache.sqoop.schema.type.Binary;
import org.apache.sqoop.schema.type.Bit;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.Date;
import org.apache.sqoop.schema.type.DateTime;
import org.apache.sqoop.schema.type.Decimal;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.apache.sqoop.schema.type.Text;
import org.apache.sqoop.schema.type.Time;
import org.apache.sqoop.schema.type.Unknown;

public class OracleSqlTypesUtils {

  private static final Logger LOG =
      Logger.getLogger(OracleSqlTypesUtils.class);

  public static Column sqlTypeToSchemaType(int sqlType, String columnName,
      int precision, int scale) {
    Column result = null;
    switch (sqlType) {
      case Types.SMALLINT:
      case Types.TINYINT:
        // only supports signed values
        result = new FixedPoint(columnName, 2L, true);
        break;
      case Types.INTEGER:
        // only supports signed values
        result = new FixedPoint(columnName, 4L, true);
        break;
      case Types.BIGINT:
        result = new FixedPoint(columnName, 8L, true);
        break;

      case Types.CLOB:
      case Types.VARCHAR:
      case Types.CHAR:
      case Types.LONGVARCHAR:
      case Types.NVARCHAR:
      case Types.NCHAR:
      case Types.LONGNVARCHAR:
        result = new Text(columnName);
        break;

      case Types.DATE:
        result = new Date(columnName);
        break;

      case Types.TIME:
        result = new Time(columnName, true);
        break;

      case Types.TIMESTAMP:
        result = new DateTime(columnName, true, false);
        break;

      case Types.FLOAT:
      case Types.REAL:
        result = new FloatingPoint(columnName, 4L);
        break;
      case Types.DOUBLE:
        result = new FloatingPoint(columnName, 8L);
        break;

      case Types.NUMERIC:
      case Types.DECIMAL:
        result = new Decimal(columnName, precision, scale);
        break;

      case Types.BIT:
      case Types.BOOLEAN:
        result = new Bit(columnName);
        break;

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.BLOB:
      case Types.LONGVARBINARY:
        result = new Binary(columnName);
        break;

      default:
        result = new Unknown(columnName,(long)sqlType);
    }

    if (sqlType == OracleQueries.getOracleType("TIMESTAMP")) {
      result = new DateTime(columnName, true, false);
    }

    if (sqlType == OracleQueries.getOracleType("TIMESTAMPTZ")) {
      result = new DateTime(columnName, true, true);
    }

    if (sqlType == OracleQueries.getOracleType("TIMESTAMPLTZ")) {
      result = new DateTime(columnName, true, true);
    }

    /*
     * http://www.oracle.com/technology/sample_code/tech/java/sqlj_jdbc/files
     * /oracle10g/ieee/Readme.html
     *
     * BINARY_DOUBLE is a 64-bit, double-precision floating-point number
     * datatype. (IEEE 754) Each BINARY_DOUBLE value requires 9 bytes, including
     * a length byte. A 64-bit double format number X is divided as sign s 1-bit
     * exponent e 11-bits fraction f 52-bits
     *
     * BINARY_FLOAT is a 32-bit, single-precision floating-point number
     * datatype. (IEEE 754) Each BINARY_FLOAT value requires 5 bytes, including
     * a length byte. A 32-bit single format number X is divided as sign s 1-bit
     * exponent e 8-bits fraction f 23-bits
     */
    if (sqlType == OracleQueries.getOracleType("BINARY_FLOAT")) {
      // http://people.uncw.edu/tompkinsj/133/numbers/Reals.htm
      result = new FloatingPoint(columnName, 4L);
    }

    if (sqlType == OracleQueries.getOracleType("BINARY_DOUBLE")) {
      // http://people.uncw.edu/tompkinsj/133/numbers/Reals.htm
      result = new FloatingPoint(columnName, 8L);
    }

    if (sqlType == OracleQueries.getOracleType("STRUCT")) {
      // E.g. URITYPE
      result = new Text(columnName);
    }

    if (result == null || result instanceof Unknown) {

      // For constant values, refer to:
      // http://oracleadvisor.com/documentation/oracle/database/11.2/
      //   appdev.112/e13995/constant-values.html#oracle_jdbc

      if (sqlType == OracleQueries.getOracleType("BFILE")
          || sqlType == OracleQueries.getOracleType("NCLOB")
          || sqlType == OracleQueries.getOracleType("NCHAR")
          || sqlType == OracleQueries.getOracleType("NVARCHAR")
          || sqlType == OracleQueries.getOracleType("ROWID")
          || sqlType == OracleQueries.getOracleType("INTERVALYM")
          || sqlType == OracleQueries.getOracleType("INTERVALDS")
          || sqlType == OracleQueries.getOracleType("OTHER")) {
        result = new Text(columnName);
      }

    }

    if (result == null || result instanceof Unknown) {
      LOG.warn(String.format("%s should be updated to cater for data-type: %d",
          OracleUtilities.getCurrentMethodName(), sqlType));
    }

    return result;
  }

}
