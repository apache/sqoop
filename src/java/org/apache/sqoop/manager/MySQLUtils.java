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

package org.apache.sqoop.manager;

import static com.cloudera.sqoop.lib.DelimiterSet.NULL_CHAR;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import com.cloudera.sqoop.config.ConfigurationConstants;
import com.cloudera.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.util.DirectImportUtils;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.mapreduce.db.DBConfiguration;

/**
 * Helper methods and constants for MySQL imports/exports.
 */
public final class MySQLUtils {

  private MySQLUtils() {
  }

  public static final Log LOG = LogFactory.getLog(MySQLUtils.class.getName());

  public static final String MYSQL_DUMP_CMD = "mysqldump";
  public static final String MYSQL_IMPORT_CMD = "mysqlimport";

  public static final String OUTPUT_FIELD_DELIM_KEY =
      DelimiterSet.OUTPUT_FIELD_DELIM_KEY;
  public static final String OUTPUT_RECORD_DELIM_KEY =
      DelimiterSet.OUTPUT_RECORD_DELIM_KEY;
  public static final String OUTPUT_ENCLOSED_BY_KEY =
      DelimiterSet.OUTPUT_ENCLOSED_BY_KEY;
  public static final String OUTPUT_ESCAPED_BY_KEY =
      DelimiterSet.OUTPUT_ESCAPED_BY_KEY;
  public static final String OUTPUT_ENCLOSE_REQUIRED_KEY =
      DelimiterSet.OUTPUT_ENCLOSE_REQUIRED_KEY;

  public static final String TABLE_NAME_KEY =
      ConfigurationHelper.getDbInputTableNameProperty();
  public static final String CONNECT_STRING_KEY =
      ConfigurationHelper.getDbUrlProperty();
  public static final String USERNAME_KEY =
      ConfigurationHelper.getDbUsernameProperty();
  public static final String WHERE_CLAUSE_KEY =
      ConfigurationHelper.getDbInputConditionsProperty();

  public static final String EXTRA_ARGS_KEY =
      "sqoop.mysql.extra.args";

  public static final String MYSQL_DEFAULT_CHARSET = "ISO_8859_1";

  /**
   * @return true if the user's output delimiters match those used by mysqldump.
   * fields: ,
   * lines: \n
   * optional-enclose: \'
   * escape: \\
   */
  public static boolean outputDelimsAreMySQL(Configuration conf) {
    return ',' == (char) conf.getInt(OUTPUT_FIELD_DELIM_KEY, NULL_CHAR)
        && '\n' == (char) conf.getInt(OUTPUT_RECORD_DELIM_KEY, NULL_CHAR)
        && '\'' == (char) conf.getInt(OUTPUT_ENCLOSED_BY_KEY, NULL_CHAR)
        && '\\' == (char) conf.getInt(OUTPUT_ESCAPED_BY_KEY, NULL_CHAR)
        && !conf.getBoolean(OUTPUT_ENCLOSE_REQUIRED_KEY, false);
  }

  /**
   * Writes the user's password to a tmp file with 0600 permissions.
   * @return the filename used.
   */
  public static String writePasswordFile(Configuration conf)
      throws IOException {
    // Create the temp file to hold the user's password.
    String tmpDir = conf.get(
        ConfigurationConstants.PROP_JOB_LOCAL_DIRECTORY, "/tmp/");
    File tempFile = File.createTempFile("mysql-cnf", ".cnf", new File(tmpDir));

    // Make the password file only private readable.
    DirectImportUtils.setFilePermissions(tempFile, "0600");

    // If we're here, the password file is believed to be ours alone.  The
    // inability to set chmod 0600 inside Java is troublesome. We have to
    // trust that the external 'chmod' program in the path does the right
    // thing, and returns the correct exit status. But given our inability to
    // re-read the permissions associated with a file, we'll have to make do
    // with this.
    String password = DBConfiguration.getPassword((JobConf) conf);
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(tempFile)));
    w.write("[client]\n");
    w.write("password=" + password + "\n");
    w.close();

    return tempFile.toString();
  }
}
