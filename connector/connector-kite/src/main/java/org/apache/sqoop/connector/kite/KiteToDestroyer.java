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
package org.apache.sqoop.connector.kite;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.apache.sqoop.connector.common.FileFormat;
import org.apache.sqoop.connector.kite.configuration.ConfigUtil;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.connector.kite.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;
import org.apache.sqoop.schema.Schema;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;

/**
 * This classes allows connector to define work to complete execution.
 *
 * When import is done successfully, temporary created datasets will be merged.
 * In case of errors, they will be removed physically.
 */
public class KiteToDestroyer extends Destroyer<LinkConfiguration,
    ToJobConfiguration> {

  private static final Logger LOG = Logger.getLogger(KiteToDestroyer.class);

  @Override
  public void destroy(DestroyerContext context,
      LinkConfiguration linkConfig, ToJobConfiguration toJobConfig) {
    LOG.info("Running Kite connector destroyer");
    String uri = ConfigUtil.buildDatasetUri(
        linkConfig.linkConfig, toJobConfig.toJobConfig);

    if (ConfigUtil.isHBaseJob(toJobConfig.toJobConfig)) {
      destroyHBaseJob(context, uri, toJobConfig);
    } else {
      destroyHdfsJob(context, uri, toJobConfig);
    }
  }

  private void destroyHBaseJob(DestroyerContext context, String uri,
      ToJobConfiguration toJobConfig) {
    // TODO: SQOOP-1948
  }

  private void destroyHdfsJob(DestroyerContext context, String uri,
      ToJobConfiguration toJobConfig) {
    String[] tempUris = KiteDatasetExecutor.listTemporaryDatasetUris(uri);
    if (context.isSuccess()) {
      KiteDatasetExecutor executor = getExecutor(
          uri, context.getSchema(), toJobConfig.toJobConfig.fileFormat);
      for (String tempUri : tempUris) {
        executor.mergeDataset(tempUri);
        LOG.info(String.format("Temporary dataset %s has been merged", tempUri));
      }
    } else {
      for (String tempUri : tempUris) {
        Datasets.delete(tempUri);
        LOG.warn(String.format("Failed to import. " +
            "Temporary dataset %s has been deleted", tempUri));
      }
    }
  }

  @VisibleForTesting
  KiteDatasetExecutor getExecutor(String uri, Schema schema,
      FileFormat format) {
    Dataset<GenericRecord> dataset =
        KiteDatasetExecutor.createDataset(uri, schema, format);
    return new KiteDatasetExecutor(dataset);
  }

}