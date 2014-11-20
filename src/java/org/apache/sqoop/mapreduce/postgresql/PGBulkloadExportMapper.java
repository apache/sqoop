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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.AutoProgressMapper;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.util.LoggingUtils;
import org.apache.sqoop.util.PostgreSQLUtils;
import org.apache.sqoop.util.Executor;
import org.apache.sqoop.util.JdbcUrl;


/**
 * Mapper that starts a 'pg_bulkload' process and uses that to export rows from
 * HDFS to a PostgreSQL database at high speed.
 *
 * map() methods are actually provided by subclasses that read from
 * SequenceFiles (containing existing SqoopRecords) or text files
 * (containing delimited lines) and deliver these results to the stream
 * used to interface with pg_bulkload.
 */
public class PGBulkloadExportMapper
    extends AutoProgressMapper<LongWritable, Writable, LongWritable, Text> {
  private Configuration conf;
  private DBConfiguration dbConf;
  private Process process;
  private OutputStream out;
  protected BufferedWriter writer;
  private Thread thread;
  protected String tmpTableName;
  private String tableName;
  private String passwordFilename;


  public PGBulkloadExportMapper() {
  }


  protected void setup(Context context)
      throws IOException, InterruptedException {
    super.setup(context);
    conf = context.getConfiguration();
    dbConf = new DBConfiguration(conf);
    tableName = dbConf.getOutputTableName();
    tmpTableName = tableName + "_" + context.getTaskAttemptID().toString();

    Connection conn = null;
    try {
      conn = dbConf.getConnection();
      conn.setAutoCommit(false);
      if (conf.getBoolean("pgbulkload.clear.staging.table", false)) {
        StringBuffer query = new StringBuffer();
        query.append("DROP TABLE IF EXISTS ");
        query.append(tmpTableName);
        doExecuteUpdate(query.toString());
      }
      StringBuffer query = new StringBuffer();
      query.append("CREATE TABLE ");
      query.append(tmpTableName);
      query.append("(LIKE ");
      query.append(tableName);
      query.append(" INCLUDING CONSTRAINTS)");
      if (conf.get("pgbulkload.staging.tablespace") != null) {
        query.append("TABLESPACE ");
        query.append(conf.get("pgbulkload.staging.tablespace"));
      }
      doExecuteUpdate(query.toString());
      conn.commit();
    } catch (ClassNotFoundException ex) {
      LOG.error("Unable to load JDBC driver class", ex);
      throw new IOException(ex);
    } catch (SQLException ex) {
      LoggingUtils.logAll(LOG, "Unable to execute statement", ex);
      throw new IOException(ex);
    } finally {
      try {
        conn.close();
      } catch (SQLException ex) {
        LoggingUtils.logAll(LOG, "Unable to close connection", ex);
      }
    }

    try {
      ArrayList<String> args = new ArrayList<String>();
      List<String> envp = Executor.getCurEnvpStrings();
      args.add(conf.get("pgbulkload.bin", "pg_bulkload"));
      args.add("--username="
          + conf.get(DBConfiguration.USERNAME_PROPERTY));
      args.add("--dbname="
          + JdbcUrl.getDatabaseName(conf.get(DBConfiguration.URL_PROPERTY)));
      args.add("--host="
          + JdbcUrl.getHostName(conf.get(DBConfiguration.URL_PROPERTY)));
      int port = JdbcUrl.getPort(conf.get(DBConfiguration.URL_PROPERTY));
      if (port != -1) {
        args.add("--port=" + port);
      }
      args.add("--input=stdin");
      args.add("--output=" + tmpTableName);
      args.add("-o");
      args.add("TYPE=CSV");
      args.add("-o");
      args.add("DELIMITER=" + conf.get("pgbulkload.input.field.delim", ","));
      args.add("-o");
      args.add("QUOTE=" + conf.get("pgbulkload.input.enclosedby", "\""));
      args.add("-o");
      args.add("ESCAPE=" + conf.get("pgbulkload.input.escapedby", "\""));
      args.add("-o");
      args.add("CHECK_CONSTRAINTS=" + conf.get("pgbulkload.check.constraints"));
      args.add("-o");
      args.add("PARSE_ERRORS=" + conf.get("pgbulkload.parse.errors"));
      args.add("-o");
      args.add("DUPLICATE_ERRORS=" + conf.get("pgbulkload.duplicate.errors"));
      if (conf.get("pgbulkload.null.string") != null) {
        args.add("-o");
        args.add("NULL=" + conf.get("pgbulkload.null.string"));
      }
      if (conf.get("pgbulkload.filter") != null) {
        args.add("-o");
        args.add("FILTER=" + conf.get("pgbulkload.filter"));
      }
      LOG.debug("Starting pg_bulkload with arguments:");
      for (String arg : args) {
        LOG.debug("  " + arg);
      }
      if (conf.get(DBConfiguration.PASSWORD_PROPERTY) != null) {
        String tmpDir = System.getProperty("test.build.data", "/tmp/");
        if (!tmpDir.endsWith(File.separator)) {
          tmpDir = tmpDir + File.separator;
        }
        tmpDir = conf.get("job.local.dir", tmpDir);
        passwordFilename = PostgreSQLUtils.writePasswordFile(tmpDir,
            conf.get(DBConfiguration.PASSWORD_PROPERTY));
        envp.add("PGPASSFILE=" + passwordFilename);
      }
      process = Runtime.getRuntime().exec(args.toArray(new String[0]),
                                          envp.toArray(new String[0]));
      out = process.getOutputStream();
      writer = new BufferedWriter(new OutputStreamWriter(out));
      thread = new ReadThread(process.getErrorStream());
      thread.start();
    } catch (Exception e) {
      LOG.error("Can't start up pg_bulkload process", e);
      cleanup(context);
      doExecuteUpdate("DROP TABLE " + tmpTableName);
      throw new IOException(e);
    }
  }


  public void map(LongWritable key, Writable value, Context context)
    throws IOException, InterruptedException {
    try {
      String str = value.toString();
      if (value instanceof Text) {
        writer.write(str, 0, str.length());
        writer.newLine();
      } else if (value instanceof SqoopRecord) {
        writer.write(str, 0, str.length());
      }
    } catch (Exception e) {
      doExecuteUpdate("DROP TABLE " + tmpTableName);
      cleanup(context);
      throw new IOException(e);
    }
  }


  protected void cleanup(Context context)
    throws IOException, InterruptedException {
    LongWritable taskid =
        new LongWritable(context.getTaskAttemptID().getTaskID().getId());
    context.write(taskid, new Text(tmpTableName));

    if (writer != null) {
      writer.close();
    }
    if (out != null) {
      out.close();
    }
    try {
      if (thread != null) {
        thread.join();
      }
    } finally {
      // block until the process is done.
      if (null != process) {
        while (true) {
          try {
            int returnValue = process.waitFor();

            // Check pg_bulkload's process return value
            if (returnValue != 0) {
              throw new RuntimeException(
                "Unexpected return value from pg_bulkload: "+ returnValue);
            }
          } catch (InterruptedException ie) {
            // interrupted; loop around.
            LOG.debug("Caught interrupted exception waiting for process "
                + "pg_bulkload.bin to exit");
            //Clear the interrupted flag.   We have to call Thread.interrupted
            //to clear for interrupted exceptions from process.waitFor
            //See http://bugs.sun.com/view_bug.do?bug_id=6420270 for more info
            Thread.interrupted();
            continue;
          }
          break;
        }
      }
    }
    if (null != passwordFilename) {
      if (!new File(passwordFilename).delete()) {
        LOG.error("Could not remove postgresql password file "
                  + passwordFilename);
        LOG.error("You should remove this file to protect your credentials.");
      }
    }
  }


  protected int doExecuteUpdate(String query) throws IOException {
    Connection conn = null;
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
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      int ret = stmt.executeUpdate(query);
      conn.commit();
      return ret;
    } catch (SQLException ex) {
      LoggingUtils.logAll(LOG, "Unable to execute query: "  + query, ex);
      throw new IOException(ex);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          LoggingUtils.logAll(LOG, "Unable to close statement", ex);
        }
      }
      try {
        conn.close();
      } catch (SQLException ex) {
        LoggingUtils.logAll(LOG, "Unable to close connection", ex);
      }
    }
  }


  private class ReadThread extends Thread {
    private InputStream in;

    ReadThread(InputStream in) {
      this.in = in;
    }

    public void run() {
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String line = null;
      try {
        while((line = reader.readLine()) != null) {
          System.out.println(line);
        }
        reader.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
