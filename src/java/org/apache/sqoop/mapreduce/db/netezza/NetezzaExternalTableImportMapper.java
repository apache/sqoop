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

package org.apache.sqoop.mapreduce.db.netezza;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.io.NamedFifo;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.manager.DirectNetezzaManager;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.util.FileUploader;
import org.apache.sqoop.util.PerfCounters;
import org.apache.sqoop.util.TaskId;

import com.cloudera.sqoop.mapreduce.AutoProgressMapper;

/**
 * Netezza import mapper using external tables.
 */
public abstract class NetezzaExternalTableImportMapper<K, V> extends
  AutoProgressMapper<Integer, NullWritable, K, V> {
  /**
   * Create a named FIFO, and start Netezza import connected to that FIFO. A
   * File object representing the FIFO is in 'fifoFile'.
   */

  private Configuration conf;
  private DBConfiguration dbc;
  private File fifoFile;
  private int numMappers;
  private Connection con;
  private BufferedReader recordReader;
  public static final Log LOG = LogFactory
    .getLog(NetezzaExternalTableImportMapper.class.getName());
  private NetezzaJDBCStatementRunner extTableThread;
  private PerfCounters counter;
  private String localLogDir = null;
  private String logDir = null;
  private File taskAttemptDir = null;
  private String getSqlStatement(int myId) throws IOException {

    char fd = (char) conf.getInt(DelimiterSet.OUTPUT_FIELD_DELIM_KEY, ',');
    char qc = (char) conf.getInt(DelimiterSet.OUTPUT_ENCLOSED_BY_KEY, 0);
    char ec = (char) conf.getInt(DelimiterSet.OUTPUT_ESCAPED_BY_KEY, 0);

    String nullValue = conf.get(DirectNetezzaManager.NETEZZA_NULL_VALUE);

    boolean ctrlChars =
        conf.getBoolean(DirectNetezzaManager.NETEZZA_CTRL_CHARS_OPT, false);
    boolean truncString =
        conf.getBoolean(DirectNetezzaManager.NETEZZA_TRUNC_STRING_OPT, false);

    int errorThreshold = conf.getInt(
      DirectNetezzaManager.NETEZZA_ERROR_THRESHOLD_OPT, 1);
    String logDir = conf.get(DirectNetezzaManager.NETEZZA_LOG_DIR_OPT);
    String[] cols = dbc.getOutputFieldNames();
    String inputConds = dbc.getInputConditions();
    StringBuilder sqlStmt = new StringBuilder(2048);

    sqlStmt.append("CREATE EXTERNAL TABLE '");
    sqlStmt.append(fifoFile.getAbsolutePath());
    sqlStmt.append("' USING (REMOTESOURCE 'JDBC' ");
    sqlStmt.append(" BOOLSTYLE 'T_F' ");
    sqlStmt.append(" CRINSTRING FALSE ");
    if (ctrlChars) {
      sqlStmt.append(" CTRLCHARS TRUE ");
    }
    if (truncString) {
      sqlStmt.append(" TRUNCSTRING TRUE ");
    }
    sqlStmt.append(" DELIMITER ");
    sqlStmt.append(Integer.toString(fd));
    sqlStmt.append(" ENCODING 'internal' ");
    if (ec > 0) {
      sqlStmt.append(" ESCAPECHAR '\\' ");
    }
    sqlStmt.append(" FORMAT 'Text' ");
    sqlStmt.append(" INCLUDEZEROSECONDS TRUE ");
    sqlStmt.append(" NULLVALUE '");
    if (nullValue != null) {
      sqlStmt.append(nullValue);
    } else {
      sqlStmt.append("null");
    }
    sqlStmt.append("' ");
    if (qc > 0) {
      switch (qc) {
        case '\'':
          sqlStmt.append(" QUOTEDVALUE SINGLE ");
          break;
        case '\"':
          sqlStmt.append(" QUOTEDVALUE DOUBLE ");
          break;
        default:
          LOG.warn("Unsupported enclosed by character: " + qc + " - ignoring.");
      }
    }

    sqlStmt.append(" MAXERRORS ").append(errorThreshold);

    File logDirPath = new File(taskAttemptDir, localLogDir);
    logDirPath.mkdirs();
    if (logDirPath.canWrite() && logDirPath.isDirectory()) {
       sqlStmt.append(" LOGDIR ").append(logDirPath.getAbsolutePath()).append(' ');
    } else {
        throw new IOException("Unable to create log directory specified");
    }

    sqlStmt.append(") AS SELECT ");
    if (cols == null || cols.length == 0) {
      sqlStmt.append('*');
    } else {
      sqlStmt.append(cols[0]).append(' ');
      for (int i = 0; i < cols.length; ++i) {
        sqlStmt.append(',').append(cols[i]);
      }
    }
    sqlStmt.append(" FROM ").append(dbc.getInputTableName()).append(' ');
    sqlStmt.append("WHERE (DATASLICEID % ");
    sqlStmt.append(numMappers).append(") = ").append(myId);
    if (inputConds != null && inputConds.length() > 0) {
      sqlStmt.append(" AND ( ").append(inputConds).append(')');
    }

    String stmt = sqlStmt.toString();
    LOG.debug("SQL generated for external table import for data slice " + myId
      + "=" + stmt);
    return stmt;
  }

  private void initNetezzaExternalTableImport(int myId) throws IOException {

    taskAttemptDir = TaskId.getLocalWorkPath(conf);

    this.fifoFile = new File(taskAttemptDir, ("nzexttable-" + myId + ".txt"));
    String filename = fifoFile.toString();
    NamedFifo nf;
    // Create the FIFO itself.
    try {
      nf = new NamedFifo(this.fifoFile);
      nf.create();
    } catch (IOException ioe) {
      // Command failed.
      LOG.error("Could not create FIFO file " + filename);
      this.fifoFile = null;
      throw new IOException(
        "Could not create FIFO for netezza external table import", ioe);
    }
    String sqlStmt = getSqlStatement(myId);
    boolean cleanup = false;
    try {
      con = dbc.getConnection();
      extTableThread = new NetezzaJDBCStatementRunner(Thread.currentThread(),
        con, sqlStmt);
    } catch (SQLException sqle) {
      cleanup = true;
      throw new IOException(sqle);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } finally {
      if (con != null && cleanup) {
        try {
          con.close();
        } catch (Exception e) {
          LOG.debug("Exception closing connection " + e.getMessage());
        }
      }
      con = null;
    }
    extTableThread.start();
    // We need to start the reader end first

    recordReader = new BufferedReader(new InputStreamReader(
      new FileInputStream(nf.getFile())));
  }

  abstract protected void writeRecord(Text text, Context context)
    throws IOException, InterruptedException;

  public void map(Integer dataSliceId, NullWritable val, Context context)
    throws IOException, InterruptedException {
    conf = context.getConfiguration();
    localLogDir =
        DirectNetezzaManager.getLocalLogDir(context.getTaskAttemptID());
    logDir = conf.get(DirectNetezzaManager.NETEZZA_LOG_DIR_OPT);

    dbc = new DBConfiguration(conf);
    numMappers = ConfigurationHelper.getConfNumMaps(conf);
    char rd = (char) conf.getInt(DelimiterSet.OUTPUT_RECORD_DELIM_KEY, '\n');
    initNetezzaExternalTableImport(dataSliceId);
    counter = new PerfCounters();
    counter.startClock();
    Text outputRecord = new Text();
    if (extTableThread.isAlive()) {
      try {
        String inputRecord = recordReader.readLine();
        while (inputRecord != null) {
          if (Thread.interrupted()) {
            if (!extTableThread.isAlive()) {
              break;
            }
          }
          outputRecord.set(inputRecord + rd);
          // May be we should set the output to be String for faster performance
          // There is no real benefit in changing it to Text and then
          // converting it back in our case
          writeRecord(outputRecord, context);
          counter.addBytes(1 + inputRecord.length());
          inputRecord = recordReader.readLine();
        }
      } finally {
        recordReader.close();
        extTableThread.join();
        counter.stopClock();
        LOG.info("Transferred " + counter.toString());
        if (extTableThread.hasExceptions()) {
          extTableThread.printException();
          throw new IOException(extTableThread.getException());
        }
        FileUploader.uploadFilesToDFS(taskAttemptDir.getAbsolutePath(),
          localLogDir, logDir, context.getJobID().toString(),
          conf);
      }
    }
  }

}
