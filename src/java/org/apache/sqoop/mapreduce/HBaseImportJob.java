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

package org.apache.sqoop.mapreduce;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.hbase.HBasePutProcessor;
import com.cloudera.sqoop.lib.FieldMapProcessor;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.DataDrivenImportJob;
import com.cloudera.sqoop.util.ImportException;

/**
 * Runs an HBase import via DataDrivenDBInputFormat to the HBasePutProcessor
 * in the DelegatingOutputFormat.
 */
public class HBaseImportJob extends DataDrivenImportJob {

  public static final Log LOG = LogFactory.getLog(
      HBaseImportJob.class.getName());

  public HBaseImportJob(final SqoopOptions opts,
      final ImportJobContext importContext) {
    super(opts, importContext.getInputFormat(), importContext);
  }

  @Override
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws IOException {
    job.setOutputKeyClass(SqoopRecord.class);
    job.setOutputValueClass(NullWritable.class);
    job.setMapperClass(getMapperClass());
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    return HBaseImportMapper.class;
  }

  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass()
      throws ClassNotFoundException {
    return DelegatingOutputFormat.class;
  }

  @Override
  protected void configureOutputFormat(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {

    // Use the DelegatingOutputFormat with the HBasePutProcessor.
    job.setOutputFormatClass(getOutputFormatClass());

    Configuration conf = job.getConfiguration();
    conf.setClass("sqoop.output.delegate.field.map.processor.class",
        HBasePutProcessor.class,
        FieldMapProcessor.class);

    // Set the HBase parameters (table, column family, row key):
    conf.set(HBasePutProcessor.TABLE_NAME_KEY, options.getHBaseTable());
    conf.set(HBasePutProcessor.COL_FAMILY_KEY, options.getHBaseColFamily());

    // What column of the input becomes the row key?
    String rowKeyCol = options.getHBaseRowKeyColumn();
    if (null == rowKeyCol) {
      // User didn't explicitly set one. If there's a split-by column set,
      // use that.
      rowKeyCol = options.getSplitByCol();
    }

    if (null == rowKeyCol) {
      // No split-by column is explicitly set.
      // If the table has a primary key, use that.
      ConnManager manager = getContext().getConnManager();
      rowKeyCol = manager.getPrimaryKey(tableName);
    }

    if (null == rowKeyCol) {
      // Give up here if this is still unset.
      throw new IOException("Could not determine the row-key column. "
          + "Use --hbase-row-key to specify the input column that "
          + "names each row.");
    }

    conf.set(HBasePutProcessor.ROW_KEY_COLUMN_KEY, rowKeyCol);
  }

  protected boolean skipDelegationTokens(Configuration conf) {
    return conf.getBoolean("sqoop.hbase.security.token.skip", false);
  }

  @Override
  /** Create the target HBase table before running the job. */
  protected void jobSetup(Job job) throws IOException, ImportException {
    Configuration conf = job.getConfiguration();
    String tableName = conf.get(HBasePutProcessor.TABLE_NAME_KEY);
    String familyName = conf.get(HBasePutProcessor.COL_FAMILY_KEY);

    if (null == tableName) {
      throw new ImportException(
          "Import to HBase error: Table name not specified");
    }

    if (null == familyName) {
      throw new ImportException(
          "Import to HBase error: Column family not specified");
    }
    Method m = null;
    try {
      m = HBaseConfiguration.class.getMethod("merge",
      Configuration.class, Configuration.class);
    } catch (NoSuchMethodException nsme) {
    }

    if (m != null) {
      // Add HBase configuration files to this conf object.

      Configuration newConf = HBaseConfiguration.create(conf);
      try {
        m.invoke(null, conf, newConf);
      } catch (Exception e) {
        throw new ImportException(e);
      }
    } else {
      HBaseConfiguration.addHbaseResources(conf);
    }

    HBaseAdmin admin = new HBaseAdmin(conf);

    if (!skipDelegationTokens(conf)) {
      // Add authentication token to the job if we're running on secure cluster.
      //
      // We're currently supporting HBase version 0.90 that do not have security
      // patches which means that it do not have required methods
      // "isSecurityEnabled" and "obtainAuthTokenForJob".
      //
      // We're using reflection API to see if those methods are available and call
      // them only if they are present.
      //
      // After we will remove support for HBase 0.90 we can simplify the code to
      // following code fragment:
      /*
      try {
        if (User.isSecurityEnabled()) {
          User user = User.getCurrent();
          user.obtainAuthTokenForJob(conf, job);
        }
      } catch(InterruptedException ex) {
        throw new ImportException("Can't get authentication token", ex);
      }
      */
      try {
        // Get method isSecurityEnabled
        Method isHBaseSecurityEnabled = User.class.getMethod(
            "isHBaseSecurityEnabled", Configuration.class);

        // Get method obtainAuthTokenForJob
        Method obtainAuthTokenForJob = User.class.getMethod(
            "obtainAuthTokenForJob", Configuration.class, Job.class);

        // Get current user
        User user = User.getCurrent();

        // Obtain security token if needed
        if ((Boolean)isHBaseSecurityEnabled.invoke(null, conf)) {
          obtainAuthTokenForJob.invoke(user, conf, job);
        }
      } catch (NoSuchMethodException e) {
        LOG.info("It seems that we're running on HBase without security"
            + " additions. Security additions will not be used during this job.");
      } catch (InvocationTargetException e) {
        throw new ImportException("Can't get authentication token", e);
      } catch (IllegalAccessException e) {
        throw new ImportException("Can't get authentication token", e);
      }
    }

    // Check to see if the table exists.
    HTableDescriptor tableDesc = null;
    byte [] familyBytes = Bytes.toBytes(familyName);
    HColumnDescriptor colDesc = new HColumnDescriptor(familyBytes);
    if (!admin.tableExists(tableName)) {
      if (options.getCreateHBaseTable()) {
        // Create the table.
        LOG.info("Creating missing HBase table " + tableName);
        tableDesc =  new HTableDescriptor(tableName);
        tableDesc.addFamily(colDesc);
        admin.createTable(tableDesc);
      } else {
        LOG.warn("Could not find HBase table " + tableName);
        LOG.warn("This job may fail. Either explicitly create the table,");
        LOG.warn("or re-run with --hbase-create-table.");
      }
    } else {
      // Table exists, so retrieve their current version
      tableDesc = admin.getTableDescriptor(Bytes.toBytes(tableName));

      // Check if current version do have specified column family
      if (!tableDesc.hasFamily(familyBytes)) {
        if (options.getCreateHBaseTable()) {
          // Create the column family.
          LOG.info("Creating missing column family " + familyName);
          admin.disableTable(tableName);
          admin.addColumn(tableName, colDesc);
          admin.enableTable(tableName);
        } else {
          LOG.warn("Could not find column family " + familyName + " in table "
            + tableName);
          LOG.warn("This job may fail. Either create the column family,");
          LOG.warn("or re-run with --hbase-create-table.");
        }
      }
    }

    // Make sure we close the connection to HBA, this is only relevant in
    // unit tests
    admin.close();

    // Make sure HBase libraries are shipped as part of the job.
    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.addDependencyJars(conf, HTable.class);

    super.jobSetup(job);
  }
}

