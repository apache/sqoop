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

package org.apache.sqoop.kudu;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kudu.Type;

import java.sql.Types;

/**
 * Provides datatype mapping between Java SQL datatypes
 * and Kudu datatypes.
 */
public final class KuduTypes {
  public static final Log LOG = LogFactory.getLog(
      KuduTypes.class.getName());

  //Prevent instantiation.
  private KuduTypes() {
  }

  /**
   * Given JDBC SQL types coming from another database, what is the best
   * mapping to a Kudu-specific type?
   */
  public static Type toKuduType(int sqlType) {

    switch (sqlType) {
      case Types.INTEGER:
      case Types.SMALLINT:
        return Type.INT32;
      case Types.VARCHAR:
      case Types.CHAR:
      case Types.LONGVARCHAR:
      case Types.NVARCHAR:
      case Types.NCHAR:
      case Types.LONGNVARCHAR:
      case Types.CLOB:
        return Type.STRING;
      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        return Type.UNIXTIME_MICROS;
      case Types.REAL:
      case Types.FLOAT:
        return Type.FLOAT;
      case Types.NUMERIC:
      case Types.DECIMAL:
      case Types.DOUBLE:
        return Type.DOUBLE;
      case Types.BIT:
      case Types.BOOLEAN:
        return Type.BOOL;
      case Types.TINYINT:
        return Type.INT8;
      case Types.BIGINT:
        return Type.INT64;
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
      case Types.BINARY:
        return Type.BINARY;

      default:
        // No Datatype mapping found
        LOG.warn("No datatype mapping found for sqlType:" + sqlType);
        return null;
    }
  }

  /**
   * Determines if an approximate data type was used for Kudu.
   * @return true if a sql type can't be translated to a precise match
   * in Hive, and we have to cast it to something more generic.
   */
  public static boolean isKuduTypeImprovised(int sqlType) {
    return sqlType == Types.DATE || sqlType == Types.TIME
        || sqlType == Types.TIMESTAMP
        || sqlType == Types.DECIMAL
        || sqlType == Types.NUMERIC;
  }
}
