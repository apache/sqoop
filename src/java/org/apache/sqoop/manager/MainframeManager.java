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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.accumulo.AccumuloUtil;
import org.apache.sqoop.hbase.HBaseUtil;
import org.apache.sqoop.mapreduce.AccumuloImportJob;
import org.apache.sqoop.mapreduce.HBaseBulkImportJob;
import org.apache.sqoop.mapreduce.HBaseImportJob;
import org.apache.sqoop.mapreduce.ImportJobBase;
import org.apache.sqoop.mapreduce.mainframe.MainframeDatasetInputFormat;
import org.apache.sqoop.mapreduce.mainframe.MainframeImportJob;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.util.ImportException;


/**
 * ConnManager implementation for mainframe datasets.
 */
public class MainframeManager extends com.cloudera.sqoop.manager.ConnManager {
  public static final String DEFAULT_DATASET_COLUMN_NAME = "DEFAULT_COLUMN";
  protected SqoopOptions options;
  private static final Log LOG
      = LogFactory.getLog(MainframeManager.class.getName());

  /**
   * Constructs the MainframeManager.
   * @param opts the SqoopOptions describing the user's requested action.
   */
  public MainframeManager(final SqoopOptions opts) {
    this.options = opts;
  }

  /**
   * Launch a MapReduce job via MainframeImportJob to read the
   * partitioned dataset with MainframeDatasetInputFormat.
   */
  @Override
  public void importTable(com.cloudera.sqoop.manager.ImportJobContext context)
      throws IOException, ImportException {
    String pdsName = context.getTableName();
    String jarFile = context.getJarFile();
    SqoopOptions opts = context.getOptions();

    context.setConnManager(this);

    ImportJobBase importer;
    if (opts.getHBaseTable() != null) {
      if (!HBaseUtil.isHBaseJarPresent()) {
        throw new ImportException("HBase jars are not present in "
            + "classpath, cannot import to HBase!");
      }
      if (!opts.isBulkLoadEnabled()) {
        importer = new HBaseImportJob(opts, context);
      } else {
        importer = new HBaseBulkImportJob(opts, context);
      }
    } else if (opts.getAccumuloTable() != null) {
      if (!AccumuloUtil.isAccumuloJarPresent()) {
        throw new ImportException("Accumulo jars are not present in "
            + "classpath, cannot import to Accumulo!");
      }
      importer = new AccumuloImportJob(opts, context);
    } else {
      // Import to HDFS.
      importer = new MainframeImportJob(opts, context);
    }

    importer.setInputFormatClass(MainframeDatasetInputFormat.class);
    importer.runImport(pdsName, jarFile, null, opts.getConf());
  }

  @Override
  public String[] getColumnNames(String tableName) {
    // default is one column for the whole record
    String[] colNames = new String[1];
    colNames[0] = DEFAULT_DATASET_COLUMN_NAME;
    return colNames;
  }

  @Override
  public Map<String, Integer> getColumnTypes(String tableName) {
    Map<String, Integer> colTypes = new HashMap<String, Integer>();
    String[] colNames = getColumnNames(tableName);
    colTypes.put(colNames[0], Types.VARCHAR);
    return colTypes;
  }

  @Override
  public void discardConnection(boolean doClose) {
    // do nothing
  }

  @Override
  public String[] listDatabases() {
    LOG.error("MainframeManager.listDatabases() not supported");
    return null;
  }

  @Override
  public String[] listTables() {
    LOG.error("MainframeManager.listTables() not supported");
    return null;
  }

  @Override
  public String getPrimaryKey(String tableName) {
    return null;
  }

  @Override
  public ResultSet readTable(String tableName, String[] columns)
      throws SQLException {
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return null;
  }

  @Override
  public void close() throws SQLException {
    release();
  }

  @Override
  public void release() {
  }

  @Override
  public String getDriverClass(){
    return "";
  }

  @Override
  public void execAndPrint(String s) {
  }

}
