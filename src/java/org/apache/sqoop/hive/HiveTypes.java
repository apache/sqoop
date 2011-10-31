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

package org.apache.sqoop.hive;

import java.sql.Types;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Defines conversion between SQL types and Hive types.
 */
public final class HiveTypes {

  public static final Log LOG = LogFactory.getLog(HiveTypes.class.getName());

  private HiveTypes() { }

  /**
   * Given JDBC SQL types coming from another database, what is the best
   * mapping to a Hive-specific type?
   */
  public static String toHiveType(int sqlType) {

      switch (sqlType) {
          case Types.INTEGER:
          case Types.SMALLINT:
              return "INT";
          case Types.VARCHAR:
          case Types.CHAR:
          case Types.LONGVARCHAR:
          case Types.NVARCHAR:
          case Types.NCHAR:
          case Types.LONGNVARCHAR:
          case Types.DATE:
          case Types.TIME:
          case Types.TIMESTAMP:
          case Types.CLOB:
              return "STRING";
          case Types.NUMERIC:
          case Types.DECIMAL:
          case Types.FLOAT:
          case Types.DOUBLE:
          case Types.REAL:
              return "DOUBLE";
          case Types.BIT:
          case Types.BOOLEAN:
              return "BOOLEAN";
          case Types.TINYINT:
              return "TINYINT";
          case Types.BIGINT:
              return "BIGINT";
          default:
        // TODO(aaron): Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT,
        // BLOB, ARRAY, STRUCT, REF, JAVA_OBJECT.
        return null;
      }
  }

  /**
   * @return true if a sql type can't be translated to a precise match
   * in Hive, and we have to cast it to something more generic.
   */
  public static boolean isHiveTypeImprovised(int sqlType) {
    return sqlType == Types.DATE || sqlType == Types.TIME
        || sqlType == Types.TIMESTAMP
        || sqlType == Types.DECIMAL
        || sqlType == Types.NUMERIC;
  }
}

