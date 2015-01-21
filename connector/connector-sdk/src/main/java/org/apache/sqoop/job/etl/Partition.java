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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A part of the input data partitioned by the Partitioner.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Partition {

  /**
   * Deserialize the fields of this partition from input.
   */
  public abstract void readFields(DataInput in) throws IOException;

  /**
   * Serialize the fields of this partition to output.
   */
  public abstract void write(DataOutput out) throws IOException;

  /**
   * Each partition must be easily serializable to human readable form so that
   * it can be logged for debugging purpose.
   *
   * @return Human readable representation
   */
  public abstract String toString();
}
