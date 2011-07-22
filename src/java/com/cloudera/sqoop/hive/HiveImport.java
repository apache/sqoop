/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.hive;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.util.Executor;
import com.cloudera.sqoop.util.LoggingAsyncSink;

/**
 * Utility to import a table into the Hive metastore. Manages the connection
 * to Hive itself as well as orchestrating the use of the other classes in this
 * package.
 */
public class HiveImport {

  public static final Log LOG = LogFactory.getLog(HiveImport.class.getName());

  private SqoopOptions options;
  private ConnManager connManager;
  private Configuration configuration;
  private boolean generateOnly;

  public HiveImport(final SqoopOptions opts, final ConnManager connMgr,
      final Configuration conf, final boolean generateOnly) {
    this.options = opts;
    this.connManager = connMgr;
    this.configuration = conf;
    this.generateOnly = generateOnly;
  }


  /** 
   * @return the filename of the hive executable to run to do the import
   */
  private String getHiveBinPath() {
    // If the user has $HIVE_HOME set, then use $HIVE_HOME/bin/hive if it
    // exists.
    // Fall back to just plain 'hive' and hope it's in the path.

    String hiveHome = options.getHiveHome();
    if (null == hiveHome) {
      return "hive";
    }

    Path p = new Path(hiveHome);
    p = new Path(p, "bin");
    p = new Path(p, "hive");
    String hiveBinStr = p.toString();
    if (new File(hiveBinStr).exists()) {
      return hiveBinStr;
    } else {
      return "hive";
    }
  }

  /**
   * If we used a MapReduce-based upload of the data, remove the _logs dir
   * from where we put it, before running Hive LOAD DATA INPATH.
   */
  private void removeTempLogs(String tableName) throws IOException {
    FileSystem fs = FileSystem.get(configuration);
    String warehouseDir = options.getWarehouseDir();
    Path tablePath; 
    if (warehouseDir != null) {
      tablePath = new Path(new Path(warehouseDir), tableName);
    } else {
      tablePath = new Path(tableName);
    }

    Path logsPath = new Path(tablePath, "_logs");
    if (fs.exists(logsPath)) {
      LOG.info("Removing temporary files from import process: " + logsPath);
      if (!fs.delete(logsPath, true)) {
        LOG.warn("Could not delete temporary files; "
            + "continuing with import, but it may fail.");
      }
    }
  }

  /**
   * @return true if we're just generating the DDL for the import, but
   * not actually running it (i.e., --generate-only mode). If so, don't
   * do any side-effecting actions in Hive.
   */
  private boolean isGenerateOnly() {
    return generateOnly;
  }

  /**
   * @return a File object that can be used to write the DDL statement.
   * If we're in gen-only mode, this should be a file in the outdir, named
   * after the Hive table we're creating. If we're in import mode, this should
   * be a one-off temporary file.
   */
  private File getScriptFile(String outputTableName) throws IOException {
    if (!isGenerateOnly()) {
      return File.createTempFile("hive-script-", ".txt",
          new File(options.getTempDir()));
    } else {
      return new File(new File(options.getCodeOutputDir()),
          outputTableName + ".q");
    }
  }

  /**
   * Perform the import of data from an HDFS path to a Hive table.
   *
   * @param inputTableName the name of the table as loaded into HDFS
   * @param outputTableName the name of the table to create in Hive.
   * @param createOnly if true, run the CREATE TABLE statement but not
   * LOAD DATA.
   */
  public void importTable(String inputTableName, String outputTableName,
      boolean createOnly) throws IOException {

    if (!isGenerateOnly()) {
      removeTempLogs(inputTableName);
      LOG.info("Loading uploaded data into Hive");
    }

    if (null == outputTableName) {
      outputTableName = inputTableName;
    }
    LOG.debug("Hive.inputTable: " + inputTableName);
    LOG.debug("Hive.outputTable: " + outputTableName);

    // For testing purposes against our mock hive implementation, 
    // if the sysproperty "expected.script" is set, we set the EXPECTED_SCRIPT
    // environment variable for the child hive process. We also disable
    // timestamp comments so that we have deterministic table creation scripts.
    String expectedScript = System.getProperty("expected.script");
    List<String> env = Executor.getCurEnvpStrings();
    boolean debugMode = expectedScript != null;
    if (debugMode) {
      env.add("EXPECTED_SCRIPT=" + expectedScript);
      env.add("TMPDIR=" + options.getTempDir());
    }

    // generate the HQL statements to run.
    TableDefWriter tableWriter = new TableDefWriter(options, connManager,
        inputTableName, outputTableName,
        configuration, !debugMode);
    String createTableStr = tableWriter.getCreateTableStmt() + ";\n";
    String loadDataStmtStr = tableWriter.getLoadDataStmt() + ";\n";

    // write them to a script file.
    File scriptFile = getScriptFile(outputTableName);
    try {
      String filename = scriptFile.toString();
      BufferedWriter w = null;
      try {
        FileOutputStream fos = new FileOutputStream(scriptFile);
        w = new BufferedWriter(new OutputStreamWriter(fos));
        w.write(createTableStr, 0, createTableStr.length());
        if (!createOnly) {
          w.write(loadDataStmtStr, 0, loadDataStmtStr.length());
        }
      } catch (IOException ioe) {
        LOG.error("Error writing Hive load-in script: " + ioe.toString());
        ioe.printStackTrace();
        throw ioe;
      } finally {
        if (null != w) {
          try {
            w.close();
          } catch (IOException ioe) {
            LOG.warn("IOException closing stream to Hive script: "
                + ioe.toString());
          }
        }
      }

      if (!isGenerateOnly()) {
        // run Hive on the script and note the return code.
        String hiveExec = getHiveBinPath();
        ArrayList<String> args = new ArrayList<String>();
        args.add(hiveExec);
        args.add("-f");
        args.add(filename);

        LoggingAsyncSink logSink = new LoggingAsyncSink(LOG);
        int ret = Executor.exec(args.toArray(new String[0]),
            env.toArray(new String[0]), logSink, logSink);
        if (0 != ret) {
          throw new IOException("Hive exited with status " + ret);
        }

        LOG.info("Hive import complete.");
      }
    } finally {
      if (!isGenerateOnly()) {
        // User isn't interested in saving the DDL. Remove the file.
        if (!scriptFile.delete()) {
          LOG.warn("Could not remove temporary file: " + scriptFile.toString());
          // try to delete the file later.
          scriptFile.deleteOnExit();
        }
      }
    }
  }
}

