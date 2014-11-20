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

package org.apache.sqoop.mapreduce.postgresql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.sqoop.mapreduce.AutoProgressReducer;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.util.LoggingUtils;


/**
 * Reducer for transfering data from temporary table to destination.
 * Reducer drops all temporary tables if all data successfully transfered.
 * Temporary tables is not dropptd in error case for manual retry.
 */
public class PGBulkloadExportReducer
    extends AutoProgressReducer<LongWritable, Text,
                                NullWritable, NullWritable> {

  public static final Log LOG =
      LogFactory.getLog(PGBulkloadExportReducer.class.getName());
  private Configuration conf;
  private DBConfiguration dbConf;
  private Connection conn;
  private String tableName;


  protected void setup(Context context)
      throws IOException, InterruptedException {
    conf = context.getConfiguration();
    dbConf = new DBConfiguration(conf);
    tableName = dbConf.getOutputTableName();
    try {
      conn = dbConf.getConnection();
      conn.setAutoCommit(false);
    } catch (ClassNotFoundException ex) {
      LOG.error("Unable to load JDBC driver class", ex);
      throw new IOException(ex);
    } catch (SQLException ex) {
      LoggingUtils.logAll(LOG, "Unable to connect to database", ex);
      throw new IOException(ex);
    }
  }


  @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      for (Text value : values) {
        int inserted = stmt.executeUpdate("INSERT INTO " + tableName
                                          + " ( SELECT * FROM " + value + " )");
        stmt.executeUpdate("DROP TABLE " + value);
      }
    } catch (SQLException ex) {
      LoggingUtils.logAll(LOG, "Unable to execute create query.", ex);
      throw new IOException(ex);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          LoggingUtils.logAll(LOG, "Unable to close statement", ex);
        }
      }
    }
  }


  protected void cleanup(Context context)
    throws IOException, InterruptedException {
    try {
      conn.commit();
      conn.close();
    } catch (SQLException ex) {
      LoggingUtils.logAll(LOG, "Unable to load JDBC driver class", ex);
      throw new IOException(ex);
    }
  }

}
