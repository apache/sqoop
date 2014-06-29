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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat;

class OraOopDBInputSplit extends DBInputFormat.DBInputSplit {

  private int splitId;
  private double totalNumberOfBlocksInAllSplits;
  private String splitLocation;
  private List<OraOopOracleDataChunk> oracleDataChunks;

  // NB: Update write(), readFields() and getDebugDetails() if you add fields
  // here.

  public OraOopDBInputSplit() {

    this.splitId = -1;
    this.splitLocation = "";
    this.oracleDataChunks = new ArrayList<OraOopOracleDataChunk>();
  }

  public OraOopDBInputSplit(List<OraOopOracleDataChunk> dataChunks) {

    setOracleDataChunks(dataChunks);
  }

  public void setOracleDataChunks(List<OraOopOracleDataChunk> dataChunks) {

    this.oracleDataChunks = dataChunks;
  }

  public List<OraOopOracleDataChunk> getDataChunks() {

    return this.oracleDataChunks;
  }

  public int getNumberOfDataChunks() {

    if (this.getDataChunks() == null) {
      return 0;
    } else {
      return this.getDataChunks().size();
    }
  }

  @Override
  public String[] getLocations() throws IOException {

    if (this.splitLocation.isEmpty()) {
      return new String[] {};
    } else {
      return new String[] { this.splitLocation };
    }

  }

  /**
   * @return The total number of blocks within the data-chunks of this split
   */
  @Override
  public long getLength() {

    return this.getTotalNumberOfBlocksInThisSplit();
  }

  public int getTotalNumberOfBlocksInThisSplit() {

    if (this.getNumberOfDataChunks() == 0) {
      return 0;
    }

    int result = 0;
    for (OraOopOracleDataChunk dataChunk : this.getDataChunks()) {
      result += dataChunk.getNumberOfBlocks();
    }

    return result;
  }

  public OraOopOracleDataChunk findDataChunkById(String id) {

    for (OraOopOracleDataChunk dataChunk : this.getDataChunks()) {
      if (dataChunk.getId().equals(id)) {
        return dataChunk;
      }
    }
    return null;
  }

  @Override
  /** {@inheritDoc} */
  public void write(DataOutput output) throws IOException {

    output.writeInt(splitId);

    if (this.oracleDataChunks == null) {
      output.writeInt(0);
    } else {
      output.writeInt(this.oracleDataChunks.size());
      for (OraOopOracleDataChunk dataChunk : this.oracleDataChunks) {
        Text.writeString(output, dataChunk.getClass().getName());
        dataChunk.write(output);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  /** {@inheritDoc} */
  public void readFields(DataInput input) throws IOException {

    this.splitId = input.readInt();

    int dataChunkCount = input.readInt();
    if (dataChunkCount == 0) {
      this.oracleDataChunks = null;
    } else {
      Class<? extends OraOopOracleDataChunk> dataChunkClass;
      OraOopOracleDataChunk dataChunk;
      this.oracleDataChunks =
          new ArrayList<OraOopOracleDataChunk>(dataChunkCount);
      for (int idx = 0; idx < dataChunkCount; idx++) {
        try {
          dataChunkClass =
              (Class<? extends OraOopOracleDataChunk>) Class.forName(Text
                  .readString(input));
          dataChunk = dataChunkClass.newInstance();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        dataChunk.readFields(input);
        this.oracleDataChunks.add(dataChunk);
      }
    }
  }

  public String getDebugDetails() {

    StringBuilder result = new StringBuilder();

    if (this.getNumberOfDataChunks() == 0) {
      result.append(String.format(
          "Split[%s] does not contain any Oracle data-chunks.", this.splitId));
    } else {
      result.append(String.format(
          "Split[%s] includes the Oracle data-chunks:\n", this.splitId));
      for (OraOopOracleDataChunk dataChunk : getDataChunks()) {
        result.append(dataChunk.toString());
      }
    }
    return result.toString();
  }

  protected int getSplitId() {
    return this.splitId;
  }

  protected void setSplitId(int newSplitId) {
    this.splitId = newSplitId;
  }

  protected void setSplitLocation(String newSplitLocation) {
    this.splitLocation = newSplitLocation;
  }

  protected void setTotalNumberOfBlocksInAllSplits(
      int newTotalNumberOfBlocksInAllSplits) {
    this.totalNumberOfBlocksInAllSplits = newTotalNumberOfBlocksInAllSplits;
  }

}
