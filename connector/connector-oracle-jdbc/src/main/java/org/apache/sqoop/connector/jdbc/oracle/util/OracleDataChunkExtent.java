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

package org.apache.sqoop.connector.jdbc.oracle.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.sqoop.connector.jdbc.oracle.OracleJdbcConnectorConstants;

/**
 * Data should be split by extent for ROWID scans.
 */
public class OracleDataChunkExtent extends OracleDataChunk {

  private int oracleDataObjectId;
  private int relativeDatafileNumber;
  private long startBlockNumber;
  private long finishBlockNumber;

  public OracleDataChunkExtent() {

  }

  public OracleDataChunkExtent(String id, int oracleDataObjectId,
      int relativeDatafileNumber, long startBlockNumber,
      long finishBlockNumber) {

    this.setId(id);
    this.oracleDataObjectId = oracleDataObjectId;
    this.relativeDatafileNumber = relativeDatafileNumber;
    this.startBlockNumber = startBlockNumber;
    this.finishBlockNumber = finishBlockNumber;
  }

  @Override
  public String getWhereClause() {
    return String.format(
        "(rowid >= dbms_rowid.rowid_create(%d, %d, %d, %d, %d)",
        OracleJdbcConnectorConstants.Oracle.ROWID_EXTENDED_ROWID_TYPE,
        this.oracleDataObjectId, this.relativeDatafileNumber,
        this.startBlockNumber, 0)
        + String.format(
            " AND rowid <= dbms_rowid.rowid_create(%d, %d, %d, %d, %d))",
            OracleJdbcConnectorConstants.Oracle.ROWID_EXTENDED_ROWID_TYPE,
            this.oracleDataObjectId, this.relativeDatafileNumber,
            this.finishBlockNumber,
            OracleJdbcConnectorConstants.Oracle.ROWID_MAX_ROW_NUMBER_PER_BLOCK);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(this.getId());
    output.writeInt(this.oracleDataObjectId);
    output.writeInt(this.relativeDatafileNumber);
    output.writeLong(this.startBlockNumber);
    output.writeLong(this.finishBlockNumber);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    this.setId(input.readUTF());
    this.oracleDataObjectId = input.readInt();
    this.relativeDatafileNumber = input.readInt();
    this.startBlockNumber = input.readLong();
    this.finishBlockNumber = input.readLong();
  }

  @Override
  public long getNumberOfBlocks() {
    if (this.finishBlockNumber == 0L && this.startBlockNumber == 0L) {
      return 0;
    } else {
      return (this.finishBlockNumber - this.startBlockNumber) + 1L;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("\n\t%s = %s", "id", getId()));
    sb.append(String.format("\n\t%s = %s",
        "oracleDataObjectId", oracleDataObjectId));
    sb.append(String.format("\n\t%s = %s",
        "relativeDatafileNumber", relativeDatafileNumber));
    sb.append(String.format("\n\t%s = %s",
        "startBlockNumber", startBlockNumber));
    sb.append(String.format("\n\t%s = %s",
        "finishBlockNumber", finishBlockNumber));
    return sb.toString();
  }

}
