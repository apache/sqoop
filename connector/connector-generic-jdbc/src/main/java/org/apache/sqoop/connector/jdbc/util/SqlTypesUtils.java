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
package org.apache.sqoop.connector.jdbc.util;

import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.Binary;
import org.apache.sqoop.schema.type.Bit;
import org.apache.sqoop.schema.type.Date;
import org.apache.sqoop.schema.type.DateTime;
import org.apache.sqoop.schema.type.Decimal;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.apache.sqoop.schema.type.Text;
import org.apache.sqoop.schema.type.Time;
import org.apache.sqoop.schema.type.Unknown;

import java.sql.Types;

/**
 * Utility class to work with SQL types.
 */
public class SqlTypesUtils {

  /**
   * Convert given java.sql.Types number into internal data type.
   *
   * @param sqlType java.sql.Types constant
   * @param columnName column name
   *
   * @return Concrete Column implementation
   */
  public static Column sqlTypeToSchemaType(int sqlType, String columnName, int precision, int scale) {
    switch (sqlType) {
      case Types.SMALLINT:
      case Types.TINYINT:
        // only supports signed values
        return new FixedPoint(columnName, 2L, true);
      case Types.INTEGER:
        // only supports signed values
        return new FixedPoint(columnName, 4L, true);
      case Types.BIGINT:
        return new FixedPoint(columnName, 8L, true);

      case Types.CLOB:
      case Types.VARCHAR:
      case Types.CHAR:
      case Types.LONGVARCHAR:
      case Types.NVARCHAR:
      case Types.NCHAR:
      case Types.LONGNVARCHAR:
        return new Text(columnName);

      case Types.DATE:
        return new Date(columnName);

      case Types.TIME:
        return new Time(columnName, true);

      case Types.TIMESTAMP:
        return new DateTime(columnName, true, false);

      case Types.FLOAT:
      case Types.REAL:
        return new FloatingPoint(columnName, 4L);
      case Types.DOUBLE:
        return new FloatingPoint(columnName, 8L);

      case Types.NUMERIC:
      case Types.DECIMAL:
        return new Decimal(columnName, precision, scale);

      case Types.BIT:
      case Types.BOOLEAN:
        return new Bit(columnName);

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.BLOB:
      case Types.LONGVARBINARY:
        return new Binary(columnName);

      default:
        return new Unknown(columnName,(long)sqlType);
    }
  }

  private SqlTypesUtils() {
    // Instantiation is prohibited
  }
}
