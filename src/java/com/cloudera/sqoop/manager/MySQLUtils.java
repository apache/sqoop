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

package com.cloudera.sqoop.manager;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public final class MySQLUtils {

  private MySQLUtils() {
  }

  public static final String MYSQL_DUMP_CMD =
      org.apache.sqoop.manager.MySQLUtils.MYSQL_DUMP_CMD;
  public static final String MYSQL_IMPORT_CMD =
      org.apache.sqoop.manager.MySQLUtils.MYSQL_IMPORT_CMD;
  public static final String OUTPUT_FIELD_DELIM_KEY =
      org.apache.sqoop.manager.MySQLUtils.OUTPUT_FIELD_DELIM_KEY;
  public static final String OUTPUT_RECORD_DELIM_KEY =
      org.apache.sqoop.manager.MySQLUtils.OUTPUT_RECORD_DELIM_KEY;
  public static final String OUTPUT_ENCLOSED_BY_KEY =
      org.apache.sqoop.manager.MySQLUtils.OUTPUT_ENCLOSED_BY_KEY;
  public static final String OUTPUT_ESCAPED_BY_KEY =
      org.apache.sqoop.manager.MySQLUtils.OUTPUT_ESCAPED_BY_KEY;
  public static final String OUTPUT_ENCLOSE_REQUIRED_KEY =
      org.apache.sqoop.manager.MySQLUtils.OUTPUT_ENCLOSE_REQUIRED_KEY;
  public static final String TABLE_NAME_KEY =
      org.apache.sqoop.manager.MySQLUtils.TABLE_NAME_KEY;
  public static final String CONNECT_STRING_KEY =
      org.apache.sqoop.manager.MySQLUtils.CONNECT_STRING_KEY;
  public static final String USERNAME_KEY =
      org.apache.sqoop.manager.MySQLUtils.USERNAME_KEY;
  public static final String WHERE_CLAUSE_KEY =
      org.apache.sqoop.manager.MySQLUtils.WHERE_CLAUSE_KEY;
  public static final String EXTRA_ARGS_KEY =
      org.apache.sqoop.manager.MySQLUtils.EXTRA_ARGS_KEY;
  public static final String MYSQL_DEFAULT_CHARSET =
      org.apache.sqoop.manager.MySQLUtils.MYSQL_DEFAULT_CHARSET;

  public static boolean outputDelimsAreMySQL(Configuration conf) {
    return org.apache.sqoop.manager.MySQLUtils.outputDelimsAreMySQL(conf);
  }

  public static String writePasswordFile(Configuration conf)
      throws IOException {
    return org.apache.sqoop.manager.MySQLUtils.writePasswordFile(conf);
  }

}

