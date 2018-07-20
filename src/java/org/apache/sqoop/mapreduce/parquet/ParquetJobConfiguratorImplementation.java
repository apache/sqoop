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

package org.apache.sqoop.mapreduce.parquet;

import org.apache.sqoop.mapreduce.parquet.hadoop.HadoopParquetJobConfiguratorFactory;

/**
 * An enum containing all the implementations available for {@link ParquetJobConfiguratorFactory}.
 * The enumeration constants are also used to instantiate concrete {@link ParquetJobConfiguratorFactory} objects.
 */
public enum ParquetJobConfiguratorImplementation {
  HADOOP(HadoopParquetJobConfiguratorFactory.class);

  private Class<? extends ParquetJobConfiguratorFactory> configuratorFactoryClass;

  ParquetJobConfiguratorImplementation(Class<? extends ParquetJobConfiguratorFactory> configuratorFactoryClass) {
    this.configuratorFactoryClass = configuratorFactoryClass;
  }

  public ParquetJobConfiguratorFactory createFactory() {
    try {
      return configuratorFactoryClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Could not instantiate factory class: " + configuratorFactoryClass, e);
    }
  }
}
