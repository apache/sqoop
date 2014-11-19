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

package org.apache.sqoop.manager.oracle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

/**
 * Data should be split by partition.
 */
public class OraOopOracleDataChunkPartition extends OraOopOracleDataChunk {

  private boolean isSubPartition;
  private long blocks;

  OraOopOracleDataChunkPartition() {

  }

  OraOopOracleDataChunkPartition(String partitionName, boolean isSubPartition,
      long blocks) {
    this.setId(partitionName);
    this.isSubPartition = isSubPartition;
    this.blocks = blocks;
  }

  @Override
  public long getNumberOfBlocks() {
    return this.blocks;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    Text.writeString(output, this.getId());
    output.writeBoolean(this.isSubPartition);
    output.writeLong(this.blocks);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    this.setId(Text.readString(input));
    this.isSubPartition = input.readBoolean();
    this.blocks = input.readLong();
  }

  @Override
  public String getPartitionClause() {
    StringBuilder sb = new StringBuilder();
    sb.append(" ");
    if (this.isSubPartition) {
      sb.append("SUBPARTITION");
    } else {
      sb.append("PARTITION");
    }
    sb.append("(\"").append(this.getId()).append("\")");
    return sb.toString();
  }

}
