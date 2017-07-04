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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.kudu.KuduConstants;
import org.apache.sqoop.kudu.KuduMutationProcessor;
import org.apache.sqoop.kudu.KuduTableWriter;
import org.apache.sqoop.kudu.KuduUtil;
import org.apache.kudu.client.KuduClient;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.lib.FieldMapProcessor;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ImportException;

/**
 * Runs an Kudu import via DataDrivenDBInputFormat to the KuduMutationProcessor
 * in the DelegatingOutputFormat.
 */
public class KuduImportJob extends DataDrivenImportJob {

  public static final Log LOG = LogFactory.getLog(KuduImportJob.class
      .getName());

  protected static SqoopOptions opts;
  protected static ConnManager connManager;

  public KuduImportJob(final SqoopOptions opts,
                       final ImportJobContext importContext) {
    super(opts, importContext.getInputFormat(), importContext);
    this.opts = opts;
    this.connManager = importContext.getConnManager();
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
    return KuduImportMapper.class;
  }

  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass()
      throws ClassNotFoundException {
    return DelegatingOutputFormat.class;
  }

  @Override
  protected void configureOutputFormat(Job job, String tableName,
                                       String tableClassName)
      throws ClassNotFoundException, IOException {

    // Use the DelegatingOutputFormat with the KuduMutationProcessor.
    job.setOutputFormatClass(getOutputFormatClass());

    Configuration conf = job.getConfiguration();
    conf.setClass("sqoop.output.delegate.field.map.processor.class",
        KuduMutationProcessor.class, FieldMapProcessor.class);

    // Set the Kudu parameters (table, kudu_master_url):
    conf.set(KuduMutationProcessor.TABLE_NAME_KEY, options.getKuduTable());
    conf.set(KuduMutationProcessor.KUDU_MASTER_KEY, options.getKuduURL());

  }

  protected boolean skipDelegationTokens(Configuration conf) {
    return conf.getBoolean("sqoop.kudu.security.token.skip", true);
  }

  @SuppressWarnings("deprecation")
  @Override
  /** Create the target Kudu table before running the job. */
  protected void jobSetup(Job job) throws IOException, ImportException {
    Configuration conf = job.getConfiguration();
    String tableName = conf.get(KuduMutationProcessor.TABLE_NAME_KEY);
    String masterUrl = conf.get(KuduMutationProcessor.KUDU_MASTER_KEY);

    // Add Jars
    KuduUtil.addJars(job, opts);

    if (null == tableName) {
      throw new ImportException(
          "Import to Kudu error: Table name not specified");
    }

    if (null == masterUrl) {
      throw new ImportException(
          "Import to Kudu error: Master url not specified");
    }


    KuduClient kuduClient = new KuduClient.KuduClientBuilder(masterUrl)
        .build();

    /*if (!skipDelegationTokens(conf)) {
      // DelegationTokens not implemented in Kudu so skipping for now
    }*/


    try {
      if (!kuduClient.tableExists(tableName)) {
        if (options.getCreateKuduTable()) {

          // Need key columns to create a table
          String kuduRowKeyCols = opts.getKuduKeyCols();

          if (kuduRowKeyCols == null) {
            // kudu-key-cols was not specified
            // See if a split-by column was specified
            LOG.info(
                "Checking to see if split-by can be "
                    + "used for a row-key");
            kuduRowKeyCols = opts.getSplitByCol();
          }

          if (kuduRowKeyCols == null) {
            // Split-by-col was not specified either
            // See if we can get a primary key cols form the table
            LOG.info(
                "Checking to see if primary-key "
                    + "can be used for a row-key");
            LOG.warn("--kudu-key-cols should be used "
                + "if the source table has a multi-column "
                + "primary-key");
            kuduRowKeyCols = connManager.
                getPrimaryKey(opts.getTableName());
          }

          if (kuduRowKeyCols == null) {
            // Still could not find a row key
            // give-up and return an error
            LOG.error("Could not determine row-key-column");
            throw new IOException(
                "Could not determine the row key column "
                    + "Use the --kudu-key-cols "
                    + "<CommaSeparateKeyCols> "
                    + "option to set the key columns"
            );
          }

          if (opts.getKuduKeyCols() == null
              && kuduRowKeyCols != null) {
            LOG.info("Setting Kudu row key cols to: "
                + kuduRowKeyCols);
            opts.setKuduKeyCols(kuduRowKeyCols);
          }

          // If partition columns are not specified
          // use the row-key-cols as the partition-cols
          if (opts.getKuduPartitionCols() == null) {
            LOG.warn("--kudu-partition-cols not specified "
                + ".. Defaulting to the row key cols");
            opts.setKuduPartitionCols(opts.getKuduKeyCols());
          }

          // If number of buckets is not specified
          // use the default
          if (opts.getKuduPartitionBuckets() == null) {
            LOG.warn("--kudu-partition-buckets not specified "
                + ".. Defaulting to "
                + KuduConstants.KUDU_DEFAULT_NO_OF_BUCKETS);
            opts.setKuduPartitionBuckets(
                Integer.
                    toString(
                        KuduConstants.
                            KUDU_DEFAULT_NO_OF_BUCKETS)
            );
          } else {
            LOG.info("Setting Kudu partition buckets to "
                + opts.getKuduPartitionBuckets()
            );
          }

          // Create the table.
          LOG.info("Creating missing Kudu table " + tableName);
          KuduTableWriter kuduTableWriter
              = new KuduTableWriter(opts, connManager,
              kuduClient, opts.getTableName(), tableName, conf);
          kuduTableWriter.createKuduTable();

        } else {
          LOG.warn("Could not find Kudu table " + tableName);
          LOG.warn("This job may fail. "
              + "Either explicitly create the table,");
          LOG.warn("or re-run with --kudu-create-table.");
        }
      }

    } catch (IOException e1) {
      LOG.error(e1.getMessage());
      throw e1;
    } catch (Exception e) {
      throw new IOException(e);
    }

    try {
      kuduClient.close();
    } catch (Exception e) {
      LOG.error("Error closing kudu client");
      throw new IOException("Error closing kudu client");
    }

    super.jobSetup(job);
  }

}
