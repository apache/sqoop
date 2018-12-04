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

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.config.ConfigurationConstants;

import static org.apache.avro.LogicalTypes.Decimal;

/**
 * Defines conversion between SQL types and Hive types.
 */
public final class HiveTypes {

  private static final String HIVE_TYPE_TINYINT = "TINYINT";
  private static final String HIVE_TYPE_INT = "INT";
  private static final String HIVE_TYPE_BIGINT = "BIGINT";
  private static final String HIVE_TYPE_FLOAT = "FLOAT";
  private static final String HIVE_TYPE_DOUBLE = "DOUBLE";
  private static final String HIVE_TYPE_STRING = "STRING";
  private static final String HIVE_TYPE_BOOLEAN = "BOOLEAN";
  private static final String HIVE_TYPE_BINARY = "BINARY";
  private static final String HIVE_TYPE_DECIMAL = "DECIMAL";

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
              return HIVE_TYPE_INT;
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
              return HIVE_TYPE_STRING;
          case Types.NUMERIC:
          case Types.DECIMAL:
          case Types.FLOAT:
          case Types.DOUBLE:
          case Types.REAL:
              return HIVE_TYPE_DOUBLE;
          case Types.BIT:
          case Types.BOOLEAN:
              return HIVE_TYPE_BOOLEAN;
          case Types.TINYINT:
              return HIVE_TYPE_TINYINT;
          case Types.BIGINT:
              return HIVE_TYPE_BIGINT;
          default:
        // TODO(aaron): Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT,
        // BLOB, ARRAY, STRUCT, REF, JAVA_OBJECT.
        return null;
      }
  }

  public static String toHiveType(Schema schema, SqoopOptions options) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema subSchema : schema.getTypes()) {
        if (subSchema.getType() != Schema.Type.NULL) {
          return toHiveType(subSchema, options);
        }
      }
    }

    Schema.Type avroType = schema.getType();
    switch (avroType) {
      case BOOLEAN:
        return HIVE_TYPE_BOOLEAN;
      case INT:
        return HIVE_TYPE_INT;
      case LONG:
        return HIVE_TYPE_BIGINT;
      case FLOAT:
        return HIVE_TYPE_FLOAT;
      case DOUBLE:
        return HIVE_TYPE_DOUBLE;
      case STRING:
      case ENUM:
        return HIVE_TYPE_STRING;
      case BYTES:
        return mapToDecimalOrBinary(schema, options);
      case FIXED:
        return HIVE_TYPE_BINARY;
      default:
        throw new RuntimeException(String.format("There is no Hive type mapping defined for the Avro type of: %s ", avroType.getName()));
    }
  }

  private static String mapToDecimalOrBinary(Schema schema, SqoopOptions options) {
    boolean logicalTypesEnabled = options.getConf().getBoolean(ConfigurationConstants.PROP_ENABLE_PARQUET_LOGICAL_TYPE_DECIMAL, false);
    if (logicalTypesEnabled && schema.getLogicalType() instanceof Decimal) {
      Decimal decimal = (Decimal) schema.getLogicalType();

      // trimming precision and scale to Hive's maximum values.
      int precision = Math.min(HiveDecimal.MAX_PRECISION, decimal.getPrecision());
      if (precision < decimal.getPrecision()) {
        LOG.warn("Warning! Precision in the Hive table definition will be smaller than the actual precision of the column on storage! Hive may not be able to read data from this column.");
      }
      int scale = Math.min(HiveDecimal.MAX_SCALE, decimal.getScale());
      if (scale < decimal.getScale()) {
        LOG.warn("Warning! Scale in the Hive table definition will be smaller than the actual scale of the column on storage! Hive may not be able to read data from this column.");
      }
      return String.format("%s (%d, %d)", HIVE_TYPE_DECIMAL, precision, scale);
    }
    else {
      return HIVE_TYPE_BINARY;
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

