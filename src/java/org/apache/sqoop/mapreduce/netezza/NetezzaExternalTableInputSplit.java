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

package org.apache.sqoop.mapreduce.netezza;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Netezza dataslice specific input splitter.
 *
 */
public class NetezzaExternalTableInputSplit extends InputSplit implements
    Writable {

  public static final Log LOG = LogFactory
      .getLog(NetezzaExternalTableInputSplit.class.getName());

  private int dataSliceId; // The datasliceid associated with this split

  public NetezzaExternalTableInputSplit() {
    this.dataSliceId = 0;
  }

  public NetezzaExternalTableInputSplit(int dataSliceId) {
    this.dataSliceId = dataSliceId;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 0L;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    dataSliceId = input.readInt();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(dataSliceId);
  }

  public Integer getDataSliceId() {
    return dataSliceId;
  }

}
