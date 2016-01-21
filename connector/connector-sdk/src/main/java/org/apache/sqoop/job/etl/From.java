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

import java.io.Serializable;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * This specifies classes that perform connector-defined steps
 * within import execution:
 * Initializer
 * -> Partitioner
 * -> Extractor
 * -> (Sqoop-defined steps)
 * -> Destroyer
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class From extends Transferable implements Serializable {

  private Class<? extends Partitioner> partitioner;
  private Class<? extends Partition> partition;
  private Class<? extends Extractor> extractor;

  public From(Class<? extends Initializer> initializer,
              Class<? extends Partitioner> partitioner,
              Class<? extends Partition> partition,
              Class<? extends Extractor> extractor,
              Class<? extends Destroyer> destroyer) {
    super(initializer, destroyer);
    this.partitioner = partitioner;
    this.partition = partition;
    this.extractor = extractor;
  }

  public Class<? extends Partitioner> getPartitioner() {
    return partitioner;
  }

  public Class<? extends Partition> getPartition() {
    return partition;
  }

  public Class<? extends Extractor> getExtractor() {
    return extractor;
  }

  @Override
  public String toString() {
    return "From{" + super.toString() +
      ", partitioner=" + partitioner.getName() +
      ", partition=" + partition.getName() +
      ", extractor=" + extractor.getName() +
      '}';
  }
}
