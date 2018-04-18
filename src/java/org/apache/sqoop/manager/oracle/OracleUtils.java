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

import org.apache.avro.LogicalType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.avro.AvroUtil;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.config.ConfigurationHelper;

import java.sql.Types;

/**
 * Utility class for Oracle.
 *
 */
public final class OracleUtils {
  private static final String PERIOD_REGEX = "\\.";

  private static final String PERIOD_DELIMITER = ".";

  private static final int SCALE_VALUE_NOT_SET = -127;

  public static boolean isOracleEscapingDisabled(Configuration conf) {
      return conf.getBoolean(SqoopOptions.ORACLE_ESCAPING_DISABLED, true);
    }

    public static boolean isEscaped(final String identifier) {
        return !StringUtils.isBlank(identifier) && identifier.startsWith("\"") && identifier.endsWith("\"");
    }

    public static String escapeIdentifier(final String identifier) {
        return escapeIdentifier(identifier, false);
    }

    public static String escapeIdentifier(final String identifier, boolean escapingDisabled) {
        if (escapingDisabled || StringUtils.isBlank(identifier)) {
            return identifier;
        }

        if (isEscaped(identifier)) {
            return identifier;
        } else {
            // If format is a.b.c then each identifier has to be escaped separately to avoid "ORA-00972: identifier is
            // too long" error
            String[] parts = identifier.split(PERIOD_REGEX);

            StringBuilder escapedIdentifier = new StringBuilder();
            for(String part : parts) {
                if (StringUtils.isNotBlank(escapedIdentifier)) {
                    escapedIdentifier.append(PERIOD_DELIMITER);
                }
                escapedIdentifier.append("\"" ).append(part).append("\"");
            }
            return StringUtils.isNotBlank(escapedIdentifier) ? escapedIdentifier.toString() : null;
        }
    }

    public static String unescapeIdentifier(final String identifier) {
        if (StringUtils.isBlank(identifier)) {
            return identifier;
        }

        if (isEscaped(identifier)) {
            return identifier.substring(1, identifier.length() - 1);
        } else {
            return identifier;
        }
    }

    public static LogicalType toAvroLogicalType(int sqlType, Integer precision, Integer scale, Configuration conf) {
      switch (sqlType) {
        case Types.NUMERIC:
        case Types.DECIMAL:
          // Negative scale means that there are a couple of zeros before the decimal point.
          // We need to add it to precision as an offset because negative scales are not allowed in Avro.
          if (scale < 0 && isValidScale(scale) && isValidPrecision(precision)) {
            precision = precision - scale;
            scale = 0;
          }
          Integer configuredScale = ConfigurationHelper.getIntegerConfigIfExists(
              conf, ConfigurationConstants.PROP_AVRO_DECIMAL_SCALE);
          if (!isValidScale(scale) && configuredScale == null) {
            throw new RuntimeException("Invalid scale for Avro Schema. Please specify a default scale with the -D" +
                ConfigurationConstants.PROP_AVRO_DECIMAL_SCALE + " flag to avoid this issue.");
          }

          // AvroUtil will take care of a precision that's 0.
          return AvroUtil.createDecimalType(precision, scale, conf);
        default:
          throw new IllegalArgumentException("Cannot convert SQL type "
              + sqlType + " to avro logical type");
      }
    }

  /**
   * When the scale is not set, Oracle returns it as -127
   * @param scale
   * @return
   */
  public static boolean isValidScale(Integer scale) {
    return scale != SCALE_VALUE_NOT_SET;
  }

  /**
   * Oracle returns 0 as precision if it's not set
   * @param precision
   * @return
   */
  public static boolean isValidPrecision(Integer precision) {
    return precision >= 1;
  }
}
