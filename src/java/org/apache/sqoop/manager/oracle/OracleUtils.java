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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.SqoopOptions;

/**
 * Utility class for Oracle.
 *
 */
public final class OracleUtils {
    private static final String PERIOD_REGEX = "\\.";
    private static final String PERIOD_DELIMITER = ".";

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
}
