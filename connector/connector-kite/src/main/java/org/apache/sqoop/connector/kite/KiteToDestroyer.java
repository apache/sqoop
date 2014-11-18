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
import org.apache.log4j.Logger;
import org.apache.sqoop.connector.common.FileFormat;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.connector.kite.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;
import org.apache.sqoop.schema.Schema;

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
      LinkConfiguration linkConfig, ToJobConfiguration jobConfig) {
    LOG.info("Running Kite connector destroyer");
    String[] uris = KiteDatasetExecutor.listTemporaryDatasetUris(
        jobConfig.toJobConfig.uri);
    if (context.isSuccess()) {
      KiteDatasetExecutor executor = getExecutor(
          jobConfig.toJobConfig.uri, context.getSchema(),
          linkConfig.linkConfig.fileFormat);
      for (String uri : uris) {
        executor.mergeDataset(uri);
        LOG.info(String.format("Temporary dataset %s has been merged", uri));
      }
    } else {
      for (String uri : uris) {
        KiteDatasetExecutor.deleteDataset(uri);
        LOG.warn(String.format("Failed to import. " +
            "Temporary dataset %s has been deleted", uri));
      }
    }
  }

  @VisibleForTesting
  protected KiteDatasetExecutor getExecutor(String uri, Schema schema,
      FileFormat format) {
    return new KiteDatasetExecutor(uri, schema, format);
  }

}