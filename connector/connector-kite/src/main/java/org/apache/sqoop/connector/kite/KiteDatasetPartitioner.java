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

import org.apache.sqoop.connector.kite.configuration.ConfigUtil;
import org.apache.sqoop.connector.kite.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;

import java.util.LinkedList;
import java.util.List;

/**
 * This allows connector to define how input data from the FROM source can be
 * partitioned. The number of data partitions also determines the degree of
 * parallelism.
 */
public class KiteDatasetPartitioner extends Partitioner<LinkConfiguration,
    FromJobConfiguration> {

  @Override
  public List<Partition> getPartitions(PartitionerContext context,
      LinkConfiguration linkConfiguration, FromJobConfiguration fromJobConfig) {
    // There is no way to create partitions of an un-partitioned dataset.
    // TODO: SQOOP-1942 will create partitions, if dataset is partitioned.
    KiteDatasetPartition partition = new KiteDatasetPartition();
    String uri = ConfigUtil.buildDatasetUri(
        linkConfiguration.linkConfig, fromJobConfig.fromJobConfig.uri);
    partition.setUri(uri);

    List<Partition> partitions = new LinkedList<Partition>();
    partitions.add(partition);
    return partitions;
  }

}