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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.accumulo.AccumuloConstants;
import org.apache.sqoop.accumulo.AccumuloMutationProcessor;
import org.apache.sqoop.accumulo.AccumuloUtil;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.lib.FieldMapProcessor;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.DataDrivenImportJob;
import com.cloudera.sqoop.util.ImportException;

/**
 * Runs an Accumulo import via DataDrivenDBInputFormat to the
 * AccumuloMutationProcessor in the DelegatingOutputFormat.
 */
public class AccumuloImportJob extends DataDrivenImportJob {

  public static final Log LOG
      = LogFactory.getLog(AccumuloImportJob.class.getName());
  protected static SqoopOptions opts;

  public AccumuloImportJob(final SqoopOptions opts,
      final ImportJobContext importContext) {
    super(opts, importContext.getInputFormat(), importContext);
    this.opts = opts;
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
    return AccumuloImportMapper.class;
  }

  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass()
      throws ClassNotFoundException {
    return DelegatingOutputFormat.class;
  }

  @Override
  protected void configureOutputFormat(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {

    // Use the DelegatingOutputFormat with the AccumuloMutationProcessor.
    job.setOutputFormatClass(getOutputFormatClass());

    Configuration conf = job.getConfiguration();
    conf.setClass("sqoop.output.delegate.field.map.processor.class",
        AccumuloMutationProcessor.class, FieldMapProcessor.class);

    // Set the Accumulo parameters (table, column family, row key):
    conf.set(AccumuloConstants.ZOOKEEPERS,
        options.getAccumuloZookeepers());
    conf.set(AccumuloConstants.ACCUMULO_INSTANCE,
        options.getAccumuloInstance());
    conf.set(AccumuloConstants.ACCUMULO_USER_NAME,
        options.getAccumuloUser());
    String pw = options.getAccumuloPassword();
    if (null == pw) {
      pw = "";
    }
    conf.set(AccumuloConstants.ACCUMULO_PASSWORD, pw);
    conf.set(AccumuloConstants.TABLE_NAME_KEY,
        options.getAccumuloTable());
    conf.set(AccumuloConstants.COL_FAMILY_KEY,
        options.getAccumuloColFamily());
    conf.setLong(AccumuloConstants.BATCH_SIZE,
        options.getAccumuloBatchSize());
    conf.setLong(AccumuloConstants.MAX_LATENCY,
        options.getAccumuloMaxLatency());

    // What column of the input becomes the row key?
    String rowKeyCol = options.getAccumuloRowKeyColumn();
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
      throw new IOException(
          "Could not determine the row-key column. "
              + "Use --accumulo-row-key to specify the input column that "
              + "names each row.");
    }

    conf.set(AccumuloConstants.ROW_KEY_COLUMN_KEY, rowKeyCol);
  }

  @Override
  /** Create the target Accumulo table before running the job, if appropriate.*/
  protected void jobSetup(Job job) throws IOException, ImportException {
    Configuration conf = job.getConfiguration();
    String tableName = conf.get(AccumuloConstants.TABLE_NAME_KEY);
    String familyName = conf.get(AccumuloConstants.COL_FAMILY_KEY);
    String zookeepers = conf.get(AccumuloConstants.ZOOKEEPERS);
    String instance = conf.get(AccumuloConstants.ACCUMULO_INSTANCE);
    String user = conf.get(AccumuloConstants.ACCUMULO_USER_NAME);

    if (null == tableName) {
      throw new ImportException(
          "Import to Accumulo error: Table name not specified");
    }

    if (null == familyName) {
      throw new ImportException(
          "Import to Accumulo error: Column family not specified");
    }

    try {
      // Set up the libjars
      AccumuloUtil.addJars(job, opts);

      Instance inst = new ZooKeeperInstance(instance, zookeepers);
      String password = conf.get(AccumuloConstants.ACCUMULO_PASSWORD);
      Connector conn = inst.getConnector(user, new PasswordToken(password));
      if (!conn.tableOperations().exists(tableName)) {
        if (options.getCreateAccumuloTable()) {
          LOG.info("Table " + tableName + " doesn't exist, creating.");
          try {
            conn.tableOperations().create(tableName);
          } catch (TableExistsException e) {
            // Should only happen if the table was created
            // by another process between the existence check
            // and the create command
            LOG.info("Table " + tableName + " created by another process.");
          }
        } else {
          throw new ImportException(
              "Table "
                  + tableName
                  + " does not exist, and --accumulo-create-table "
                  + "not specified.");
        }
      }
    } catch (AccumuloException e) {
      throw new ImportException(e);
    } catch (AccumuloSecurityException e) {
      throw new ImportException(e);
    }
    super.jobSetup(job);
  }
}
