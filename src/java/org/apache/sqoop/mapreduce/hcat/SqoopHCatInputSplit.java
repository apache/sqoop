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

package org.apache.sqoop.mapreduce.hcat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hive.hcatalog.mapreduce.HCatSplit;

/**
 * An abstraction of a combined HCatSplits.
 *
 */
public class SqoopHCatInputSplit extends InputSplit implements Writable {
  private List<HCatSplit> hCatSplits;
  private String[] hCatLocations;
  private long inputLength;

  public SqoopHCatInputSplit() {
  }

  public SqoopHCatInputSplit(List<InputSplit> splits) {
    hCatSplits = new ArrayList<HCatSplit>();
    Set<String> locations = new HashSet<String>();
    for (int i = 0; i < splits.size(); ++i) {
      HCatSplit hsSplit = (HCatSplit) splits.get(i);
      hCatSplits.add(hsSplit);
      this.inputLength += hsSplit.getLength();
      locations.addAll(Arrays.asList(hsSplit.getLocations()));
    }
    this.hCatLocations = locations.toArray(new String[0]);
  }

  public int length() {
    return this.hCatSplits.size();
  }

  public HCatSplit get(int index) {
    return this.hCatSplits.get(index);
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    if (this.inputLength == 0L) {
      for (HCatSplit split : this.hCatSplits) {
        this.inputLength += split.getLength();
      }
    }
    return this.inputLength;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    if (this.hCatLocations == null) {
      Set<String> locations = new HashSet<String>();
      for (HCatSplit split : this.hCatSplits) {
        locations.addAll(Arrays.asList(split.getLocations()));
      }
      this.hCatLocations = locations.toArray(new String[0]);
    }
    return this.hCatLocations;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.inputLength);
    out.writeInt(this.hCatSplits.size());
    for (HCatSplit split : this.hCatSplits) {
      split.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.inputLength = in.readLong();
    int size = in.readInt();
    this.hCatSplits = new ArrayList<HCatSplit>(size);
    for (int i = 0; i < size; ++i) {
      HCatSplit hs = new HCatSplit();
      hs.readFields(in);
      hCatSplits.add(hs);
    }
  }
}

