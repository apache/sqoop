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

import com.cloudera.sqoop.lib.DelimiterSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.sqoop.mapreduce.AutoProgressMapper;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.util.LoggingUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.CopyIn;


/**
 * Mapper that export rows from HDFS to a PostgreSQL database at high speed
 * with PostgreSQL Copy API.
 *
 * map() methods read from SequenceFiles (containing existing SqoopRecords)
 * or text files (containing delimited lines)
 * and deliver these results to the CopyIn object of PostgreSQL JDBC.
 */
public class PostgreSQLCopyExportMapper
    extends AutoProgressMapper<LongWritable, Writable,
                               NullWritable, NullWritable> {
  public static final Log LOG =
    LogFactory.getLog(PostgreSQLCopyExportMapper.class.getName());

  private Configuration conf;
  private DBConfiguration dbConf;
  private Connection conn = null;
  private CopyIn copyin = null;
  private StringBuilder line = new StringBuilder();
  private DelimiterSet delimiters =
    new DelimiterSet(',', '\n',
                     DelimiterSet.NULL_CHAR, DelimiterSet.NULL_CHAR, false);

  public PostgreSQLCopyExportMapper() {
  }

  @Override
  protected void setup(Context context)
    throws IOException, InterruptedException {

    super.setup(context);
    conf = context.getConfiguration();
    dbConf = new DBConfiguration(conf);
    CopyManager cm = null;
    try {
      conn = dbConf.getConnection();
      cm = ((PGConnection)conn).getCopyAPI();
    } catch (ClassNotFoundException ex) {
      LOG.error("Unable to load JDBC driver class", ex);
      throw new IOException(ex);
    } catch (SQLException ex) {
      LoggingUtils.logAll(LOG, "Unable to get CopyIn", ex);
      throw new IOException(ex);
    }
    try {
      StringBuilder sql = new StringBuilder();
      sql.append("COPY ");
      sql.append(dbConf.getOutputTableName());
      sql.append(" FROM STDIN WITH (");
      sql.append(" ENCODING 'UTF-8' ");
      sql.append(", FORMAT csv ");
      sql.append(", DELIMITER ");
      sql.append("'");
      sql.append(conf.get("postgresql.input.field.delim", ","));
      sql.append("'");
      sql.append(", QUOTE ");
      sql.append("'");
      sql.append(conf.get("postgresql.input.enclosedby", "\""));
      sql.append("'");
      sql.append(", ESCAPE ");
      sql.append("'");
      sql.append(conf.get("postgresql.input.escapedby", "\""));
      sql.append("'");
      if (conf.get("postgresql.null.string") != null) {
        sql.append(", NULL ");
        sql.append("'");
        sql.append(conf.get("postgresql.null.string"));
        sql.append("'");
      }
      sql.append(")");
      LOG.debug("Starting export with copy: " + sql);
      copyin = cm.copyIn(sql.toString());
    } catch (SQLException ex) {
      LoggingUtils.logAll(LOG, "Unable to get CopyIn", ex);
      close();
      throw new IOException(ex);
    }
  }

  @Override
  public void map(LongWritable key, Writable value, Context context)
    throws IOException, InterruptedException {
    line.setLength(0);
    line.append(value.toString());
    if (value instanceof Text) {
      line.append(System.getProperty("line.separator"));
    }
    try {
      byte[]data = line.toString().getBytes("UTF-8");
      copyin.writeToCopy(data, 0, data.length);
    } catch (SQLException ex) {
      LoggingUtils.logAll(LOG, "Unable to execute copy", ex);
      close();
      throw new IOException(ex);
    }
  }

  @Override
  protected void cleanup(Context context)
    throws IOException, InterruptedException {
    try {
      copyin.endCopy();
    } catch (SQLException ex) {
      LoggingUtils.logAll(LOG, "Unable to finalize copy", ex);
      throw new IOException(ex);
    }
    close();
  }

  void close() throws IOException {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException ex) {
        LoggingUtils.logAll(LOG, "Unable to close connection", ex);
        throw new IOException(ex);
      }
    }
  }
}
