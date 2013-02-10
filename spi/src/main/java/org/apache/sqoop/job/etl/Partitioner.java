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

import java.util.List;

/**
 * This allows connector to define how input data to be partitioned.
 * The number of data partitions also determines the degree of parallelism.
 */
public abstract class Partitioner<ConnectionConfiguration, JobConfiguration> {

  /**
   * Partition input data into partitions.
   *
   * Each partition will be then processed in separate extractor.
   *
   * @param context Partitioner context object
   * @param connectionConfiguration Connection configuration
   * @param jobConfiguration Job configuration
   * @return
   */
  public abstract List<Partition> getPartitions(PartitionerContext context,
                                                ConnectionConfiguration connectionConfiguration,
                                                JobConfiguration jobConfiguration);

}
