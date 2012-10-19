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
package org.apache.sqoop.job.etl;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.CoreError;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.Exporter;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.Importer;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.EtlOptions.FormatType;
import org.apache.sqoop.job.etl.EtlOptions.JobType;
import org.apache.sqoop.job.etl.EtlOptions.StorageType;

/**
 * This class encapsulates the whole ETL framework.
 *
 * For import:
 * Initializer (connector-defined)
 * -> Partitioner (connector-defined)
 * -> Extractor (connector-defined)
 * -> Loader (framework-defined)
 * -> Destroyer (connector-defined)
 *
 * For export:
 * Initializer (connector-defined)
 * -> Partitioner (framework-defined)
 * -> Extractor (framework-defined)
 * -> Loader (connector-defined)
 * -> Destroyer (connector-defined)
 */
public class EtlFramework {

  private Class<? extends Initializer> initializer;
  private Class<? extends Partitioner> partitioner;
  private Class<? extends Extractor> extractor;
  private Class<? extends Loader> loader;
  private Class<? extends Destroyer> destroyer;

  private boolean requireFieldNames;
  private boolean requireOutputDirectory;

  private EtlOptions options;

  public EtlFramework(EtlOptions inputs) {
    this.options = inputs;
    JobType jobType = options.getJobType();
    switch (jobType) {
    case IMPORT:
      constructImport();
      break;
    case EXPORT:
      constructExport();
      break;
    default:
      throw new SqoopException(CoreError.CORE_0012, jobType.toString());
    }
  }

  public EtlOptions getOptions() {
    return options;
  }

  public Class<? extends Initializer> getInitializer() {
    return initializer;
  }

  public Class<? extends Partitioner> getPartitioner() {
    return partitioner;
  }

  public Class<? extends Extractor> getExtractor() {
    return extractor;
  }

  public Class<? extends Loader> getLoader() {
    return loader;
  }

  public Class<? extends Destroyer> getDestroyer() {
    return destroyer;
  }

  public boolean isFieldNamesRequired() {
    return requireFieldNames;
  }

  public boolean isOutputDirectoryRequired() {
    return requireOutputDirectory;
  }

  private void constructImport() {
    Importer importer = options.getConnector().getImporter();
    initializer = importer.getInitializer();
    partitioner = importer.getPartitioner();
    extractor = importer.getExtractor();
    destroyer = importer.getDestroyer();

    StorageType storageType = options.getStorageType();
    switch (storageType) {
    case HDFS:
      FormatType formatType = options.getFormatType();
      switch (formatType) {
      case TEXT:
        loader = HdfsTextImportLoader.class;
        requireOutputDirectory = true;
        break;
      case SEQUENCE:
        loader = HdfsSequenceImportLoader.class;
        requireOutputDirectory = true;
        break;
      default:
        throw new SqoopException(CoreError.CORE_0012, formatType.toString());
      }
      break;
    default:
      throw new SqoopException(CoreError.CORE_0012, storageType.toString());
    }
  }

  private void constructExport() {
    Exporter exporter = options.getConnector().getExporter();
    initializer = exporter.getInitializer();
    loader = exporter.getLoader();
    destroyer = exporter.getDestroyer();

    // FIXME: set partitioner/extractor based on storage/format types
  }

}
