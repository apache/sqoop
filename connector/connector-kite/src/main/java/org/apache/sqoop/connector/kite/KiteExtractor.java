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
import org.apache.sqoop.connector.kite.configuration.ConfigUtil;
import org.apache.sqoop.connector.kite.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;

/**
 * This allows connector to extract data from a source system based on each
 * partition.
 */
public class KiteExtractor extends Extractor<LinkConfiguration,
    FromJobConfiguration, KiteDatasetPartition> {

  private static final Logger LOG = Logger.getLogger(KiteExtractor.class);

  private long rowsRead = 0L;

  @VisibleForTesting
  KiteDatasetExecutor getExecutor(String uri) {
    Dataset<GenericRecord> dataset = Datasets.load(uri);
    return new KiteDatasetExecutor(dataset);
  }

  @Override
  public void extract(ExtractorContext context, LinkConfiguration linkConfig,
      FromJobConfiguration fromJobConfig, KiteDatasetPartition partition) {
    String uri = ConfigUtil.buildDatasetUri(
        linkConfig.linkConfig, partition.getUri());
    LOG.info("Loading data from " + uri);

    KiteDatasetExecutor executor = getExecutor(uri);
    DataWriter writer = context.getDataWriter();
    Object[] array;
    rowsRead = 0L;

    try {
      while ((array = executor.readRecord()) != null) {
        // TODO: SQOOP-1616 will cover more column data types. Use schema and do data type conversion (e.g. datatime).
        writer.writeArrayRecord(array);
        rowsRead++;
      }
    } finally {
      executor.closeReader();
    }
  }

  @Override
  public long getRowsRead() {
    return rowsRead;
  }

}