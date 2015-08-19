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

package org.apache.sqoop.common.test.db.types;

/**
 * Minimal tests to avoid NPE in AllTypesTest as TestNG is not handling empty
 * List.
 *
 * Current types and tests are imported from Derby tests. In future will be expanded
 * to include all supported types.
 * The list of all datatypes can be found here:
 * https://dev.mysql.com/doc/refman/5.6/en/data-types.html
 *
 */
public class MySQLTypeList extends DatabaseTypeList{

  public MySQLTypeList() {
    super();

    // Numeric types
    add(DatabaseType.builder("SMALLINT")
        .addExample("-32768", Integer.valueOf(-32768), "-32768")
        .addExample("-1", Integer.valueOf(-1), "-1")
        .addExample("0", Integer.valueOf(0), "0")
        .addExample("1", Integer.valueOf(1), "1")
        .addExample("32767", Integer.valueOf(32767), "32767")
        .build());
    add(DatabaseType.builder("INT")
        .addExample("-2147483648", Integer.valueOf(-2147483648), "-2147483648")
        .addExample("-1", Integer.valueOf(-1), "-1")
        .addExample("0", Integer.valueOf(0), "0")
        .addExample("1", Integer.valueOf(1), "1")
        .addExample("2147483647", Integer.valueOf(2147483647), "2147483647")
        .build());
    add(DatabaseType.builder("BIGINT")
        .addExample("-9223372036854775808", Long.valueOf(-9223372036854775808L), "-9223372036854775808")
        .addExample("-1", Long.valueOf(-1L), "-1")
        .addExample("0", Long.valueOf(0L), "0")
        .addExample("1", Long.valueOf(1L), "1")
        .addExample("9223372036854775807", Long.valueOf(9223372036854775807L), "9223372036854775807")
        .build());

    // Floating points
    add(DatabaseType.builder("DOUBLE")
        .addExample("-1.79769E+308", Double.valueOf(-1.79769E+308), "-1.79769E308")
        .addExample("1.79769E+308", Double.valueOf(1.79769E+308), "1.79769E308")
        .addExample("0", Double.valueOf(0), "0.0")
        .addExample("2.225E-307", Double.valueOf(2.225E-307), "2.225E-307")
        .addExample("-2.225E-307", Double.valueOf(-2.225E-307), "-2.225E-307")
        .build());

    // Boolean
    add(DatabaseType.builder("BOOLEAN")
        .addExample("true", Boolean.TRUE, "true")
        .addExample("false", Boolean.FALSE, "false")
        .build());

  }
}
