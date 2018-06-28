/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.mapreduce.parquet.hadoop;

import org.apache.sqoop.mapreduce.parquet.ParquetExportJobConfigurator;
import org.apache.sqoop.mapreduce.parquet.ParquetImportJobConfigurator;
import org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorFactory;
import org.apache.sqoop.mapreduce.parquet.ParquetMergeJobConfigurator;

/**
 * A concrete factory implementation which produces configurator objects using the Hadoop Parquet library.
 */
public class HadoopParquetJobConfiguratorFactory implements ParquetJobConfiguratorFactory {

  @Override
  public ParquetImportJobConfigurator createParquetImportJobConfigurator() {
    return new HadoopParquetImportJobConfigurator();
  }

  @Override
  public ParquetExportJobConfigurator createParquetExportJobConfigurator() {
    return new HadoopParquetExportJobConfigurator();
  }

  @Override
  public ParquetMergeJobConfigurator createParquetMergeJobConfigurator() {
    return new HadoopParquetMergeJobConfigurator();
  }
}
