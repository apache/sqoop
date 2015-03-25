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
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.common.FileFormat;
import org.apache.sqoop.connector.common.AvroDataTypeUtil;
import org.apache.sqoop.connector.kite.configuration.ConfigUtil;
import org.apache.sqoop.connector.kite.configuration.LinkConfig;
import org.apache.sqoop.connector.kite.util.KiteDataTypeUtil;
import org.apache.sqoop.error.code.KiteConnectorError;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Format;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * The class arranges to perform dataset operations (without thread safety
 * guarantee).
 */
public class KiteDatasetExecutor {

  private final Dataset<GenericRecord> dataset;

  private DatasetWriter<GenericRecord> writer;

  private DatasetReader<GenericRecord> reader;

  /**
   * Creates a new dataset.
   */
  public static Dataset<GenericRecord> createDataset(String uri, org.apache.sqoop.schema.Schema schema,
      FileFormat format) {
    Schema datasetSchema = KiteDataTypeUtil.createAvroSchema(schema);
    Format datasetFormat = KiteDataTypeUtil.toFormat(format);
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .property("kite.allow.csv", "true")
        .schema(datasetSchema)
        .format(datasetFormat)
        .build();
    return Datasets.create(uri, descriptor);
  }

  public KiteDatasetExecutor(Dataset<GenericRecord> dataset) {
    this.dataset = dataset;
  }

  /**
   * Writes a data record into dataset.
   *
   * Note that `closeWriter()` should be called explicitly, when no more data is
   * going to be written.
   */
  public void writeRecord(Object[] data) {
    Schema schema = dataset.getDescriptor().getSchema();
    GenericRecord record = KiteDataTypeUtil.createGenericRecord(data, schema);
    getOrNewWriter().write(record);
  }

  private DatasetWriter<GenericRecord> getOrNewWriter() {
    if (writer == null) {
      writer = dataset.newWriter();
    }
    return writer;
  }

  @VisibleForTesting
  boolean isWriterClosed() {
    return writer == null || !writer.isOpen();
  }

  /**
   * Closes the writer and releases any system resources.
   */
  public void closeWriter() {
    if (writer != null) {
      Closeables.closeQuietly(writer);
      writer = null;
    }
  }

  public Object[] readRecord() {
    if (getOrNewReader().hasNext()) {
      GenericRecord record = getOrNewReader().next();
      return AvroDataTypeUtil.extractGenericRecord(record);
    }
    return null;
  }

  private DatasetReader<GenericRecord> getOrNewReader() {
    if (reader == null) {
      reader = dataset.newReader();
    }
    return reader;
  }

  @VisibleForTesting
  boolean isReaderClosed() {
    return reader == null || !reader.isOpen();
  }

  public void closeReader() {
    if (reader != null) {
      Closeables.closeQuietly(reader);
      reader = null;
    }
  }

  /**
   * Deletes current dataset physically.
   */
  public void deleteDataset() {
    Datasets.delete(dataset.getUri().toString());
  }

  /**
   * Merges a dataset into this.
   */
  public void mergeDataset(String uri) {
    FileSystemDataset<GenericRecord> update = Datasets.load(uri);
    if (dataset instanceof FileSystemDataset) {
      ((FileSystemDataset<GenericRecord>) dataset).merge(update);
      // And let's completely drop the temporary dataset
      Datasets.delete(uri);
    } else {
      throw new SqoopException(
          KiteConnectorError.GENERIC_KITE_CONNECTOR_0000, uri);
    }
  }

  private static final String TEMPORARY_DATASET_PREFIX = "temp_";

  /**
   * Workaround for managing temporary datasets.
   */
  public static String suggestTemporaryDatasetUri(LinkConfig linkConfig, String uri) {
    if (uri.startsWith("dataset:hdfs:") || uri.startsWith("dataset:hive:")) {
      int pathStart = uri.indexOf(":") + 1;
      pathStart = uri.indexOf(":", pathStart);
      int pathEnd = uri.lastIndexOf("?");
      String[] uriParts = null;

      // Get URI parts
      if (pathEnd > -1) {
        uriParts = new String[3];
        uriParts[2] = uri.substring(pathEnd, uri.length());
      } else {
        pathEnd = uri.length();
        uriParts = new String[2];
      }
      uriParts[1] = uri.substring(pathStart, pathEnd);
      uriParts[0] = uri.substring(0, pathStart);

      // Add to path
      String temporaryDatasetName = TEMPORARY_DATASET_PREFIX + UUID.randomUUID().toString().replace("-", "");
      if (uri.startsWith("dataset:hive:")) {
        if (uriParts[1].lastIndexOf("/") > -1) {
          // Kite creates databases from namespace names in hive.
          // Re-use entire path except for dataset name (table name in hive).
          // Replace dataset name with temporary dataset name.
          uriParts[1] = uriParts[1].substring(0, uriParts[1].lastIndexOf("/")) + "/" + temporaryDatasetName;
        } else {
          uriParts[1] = ":" + temporaryDatasetName;
        }
      } else {
        uriParts[1] += "/" + temporaryDatasetName;
      }

      return StringUtils.join(uriParts, "");
    } else {
      throw new SqoopException(
          KiteConnectorError.GENERIC_KITE_CONNECTOR_0000, uri);
    }
  }

  /**
   * Workaround for managing temporary datasets.
   */
  public static String[] listTemporaryDatasetUris(String uri) {
    String repo = URIBuilder.REPO_SCHEME +
        uri.substring(URIBuilder.DATASET_SCHEME.length());
    Set<String> result = new HashSet<String>();
    for (URI match : Datasets.list(repo)) {
      if (match.toString().contains(TEMPORARY_DATASET_PREFIX)) {
        result.add(match.toString());
      }
    }
    return result.toArray(new String[result.size()]);
  }

}