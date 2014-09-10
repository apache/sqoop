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

package org.apache.sqoop.mapreduce.mainframe;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * A collection of mainframe datasets.
 *
 */
public class MainframeDatasetInputSplit extends InputSplit implements Writable {
  private List<String> mainframeDatasets;
  private String currentDataset;
  private int currentIndex;

  public MainframeDatasetInputSplit() {
    mainframeDatasets = new ArrayList<String>();
    currentDataset = null;
    currentIndex = -1;
  }

  public void addDataset(String mainframeDataset) {
    mainframeDatasets.add(mainframeDataset);
  }

  public String getCurrentDataset() {
    return currentDataset;
  }

  public String getNextDataset() {
    if (hasMore()) {
      currentIndex++;
      currentDataset = mainframeDatasets.get(currentIndex);
    } else {
      currentDataset = null;
    }
    return currentDataset;
  }

  public boolean hasMore() {
    return currentIndex < (mainframeDatasets.size() -1);
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return mainframeDatasets.size();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0]; // No locations
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(mainframeDatasets.size());
    for (String ds : mainframeDatasets) {
      dataOutput.writeUTF(ds);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int numberOfDatasets = dataInput.readInt();
    for (int i = 0; i < numberOfDatasets; i++) {
      mainframeDatasets.add(dataInput.readUTF());
    }
  }
}
