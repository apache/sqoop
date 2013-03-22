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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.sqoop.io.NamedFifo;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.manager.DirectNetezzaManager;
import org.apache.sqoop.mapreduce.SqoopMapper;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.util.PerfCounters;
import org.apache.sqoop.util.TaskId;

import com.cloudera.sqoop.lib.DelimiterSet;

/**
 * Netezza export mapper using external tables.
 */
public abstract class NetezzaExternalTableExportMapper<K, V> extends
    SqoopMapper<K, V, NullWritable, NullWritable> {
  /**
   * Create a named FIFO, and start the Netezza JDBC thread connected to that
   * FIFO. A File object representing the FIFO is in 'fifoFile'.
   */

  private Configuration conf;
  private DBConfiguration dbc;
  private File fifoFile;
  private Connection con;
  private OutputStream recordWriter;
  public static final Log LOG = LogFactory
      .getLog(NetezzaExternalTableImportMapper.class.getName());
  private NetezzaJDBCStatementRunner extTableThread;
  private PerfCounters counter;
  private DelimiterSet outputDelimiters;

  private String getSqlStatement() throws IOException {

    char fd = (char) conf.getInt(DelimiterSet.INPUT_FIELD_DELIM_KEY, ',');
    char qc = (char) conf.getInt(DelimiterSet.INPUT_ENCLOSED_BY_KEY, 0);
    char ec = (char) conf.getInt(DelimiterSet.INPUT_ESCAPED_BY_KEY, 0);

    String nullValue = conf.get(DirectNetezzaManager.NETEZZA_NULL_VALUE);

    int errorThreshold = conf.getInt(
        DirectNetezzaManager.NETEZZA_ERROR_THRESHOLD_OPT, 1);
    String logDir = conf.get(DirectNetezzaManager.NETEZZA_LOG_DIR_OPT);

    StringBuilder sqlStmt = new StringBuilder(2048);

    sqlStmt.append("INSERT INTO ");
    sqlStmt.append(dbc.getInputTableName());
    sqlStmt.append(" SELECT * FROM EXTERNAL '");
    sqlStmt.append(fifoFile.getAbsolutePath());
    sqlStmt.append("' USING (REMOTESOURCE 'JDBC' ");
    sqlStmt.append(" BOOLSTYLE 'TRUE_FALSE' ");
    sqlStmt.append(" CRINSTRING FALSE ");
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

    if (logDir != null) {
      logDir = logDir.trim();
      if (logDir.length() > 0) {
        File logDirPath = new File(logDir);
        logDirPath.mkdirs();
        if (logDirPath.canWrite() && logDirPath.isDirectory()) {
          sqlStmt.append(" LOGDIR ").append(logDir).append(' ');
        } else {
          throw new IOException("Unable to create log directory specified");
        }
      }
    }
    sqlStmt.append(")");

    String stmt = sqlStmt.toString();
    LOG.debug("SQL generated for external table export" + stmt);

    return stmt;
  }

  private void initNetezzaExternalTableExport(Context context)
      throws IOException {
    this.conf = context.getConfiguration();
    dbc = new DBConfiguration(conf);
    File taskAttemptDir = TaskId.getLocalWorkPath(conf);
    this.outputDelimiters = new DelimiterSet(',', '\n', '\000', '\\', false);
    this.fifoFile = new File(taskAttemptDir, ("nzexttable-export.txt"));
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
    String sqlStmt = getSqlStatement();
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

    counter = new PerfCounters();
    extTableThread.start();
    // We start the JDBC thread first in this case as we want the FIFO reader to
    // be running.
    recordWriter = new BufferedOutputStream(new FileOutputStream(nf.getFile()));
    counter.startClock();
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    initNetezzaExternalTableExport(context);
    if (extTableThread.isAlive()) {
      try {
        while (context.nextKeyValue()) {
          if (Thread.interrupted()) {
            if (!extTableThread.isAlive()) {
              break;
            }
          }
          map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
        cleanup(context);
      } finally {
        recordWriter.close();
        extTableThread.join();
        counter.stopClock();
        LOG.info("Transferred " + counter.toString());
        if (extTableThread.hasExceptions()) {
          extTableThread.printException();
          throw new IOException(extTableThread.getExcepton());
        }
      }
    }
  }

  protected void writeTextRecord(Text record) throws IOException,
      InterruptedException {
    String outputStr = record.toString() + "\n";
    byte[] outputBytes = outputStr.getBytes("UTF-8");
    counter.addBytes(outputBytes.length);
    recordWriter.write(outputBytes, 0, outputBytes.length);
  }

  protected void writeSqoopRecord(SqoopRecord sqr) throws IOException,
      InterruptedException {
    String outputStr = sqr.toString(this.outputDelimiters);
    byte[] outputBytes = outputStr.getBytes("UTF-8");
    counter.addBytes(outputBytes.length);
    recordWriter.write(outputBytes, 0, outputBytes.length);
  }

}
