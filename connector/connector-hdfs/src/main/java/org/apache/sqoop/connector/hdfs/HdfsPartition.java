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

package org.apache.sqoop.connector.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.sqoop.job.etl.Partition;

/**
 * This class derives mostly from CombineFileSplit of Hadoop, i.e.
 * org.apache.hadoop.mapreduce.lib.input.CombineFileSplit.
 */
public class HdfsPartition extends Partition {

  private long lenFiles;
  private int numFiles;
  private Path[] files;
  private long[] offsets;
  private long[] lengths;
  private String[] locations;

  public HdfsPartition() {}

  public HdfsPartition(Path[] files, long[] offsets,
                       long[] lengths, String[] locations) {
    for(long length : lengths) {
      this.lenFiles += length;
    }
    this.numFiles = files.length;
    this.files = files;
    this.offsets = offsets;
    this.lengths = lengths;
    this.locations = locations;
  }

  public long getLengthOfFiles() {
    return lenFiles;
  }

  public int getNumberOfFiles() {
    return numFiles;
  }

  public Path getFile(int i) {
    return files[i];
  }

  public long getOffset(int i) {
    return offsets[i];
  }

  public long getLength(int i) {
    return lengths[i];
  }

  public String[] getLocations() {
    return locations;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    numFiles = in.readInt();

    files = new Path[numFiles];
    for(int i=0; i<numFiles; i++) {
      files[i] = new Path(in.readUTF());
    }

    offsets = new long[numFiles];
    for(int i=0; i<numFiles; i++) {
      offsets[i] = in.readLong();
    }

    lengths = new long[numFiles];
    for(int i=0; i<numFiles; i++) {
      lengths[i] = in.readLong();
    }

    for(long length : lengths) {
      lenFiles += length;
    }

    int numLocations = in.readInt();
    if (numLocations == 0) {
      locations = null;
    } else {
      locations = new String[numLocations];
      for(int i=0; i<numLocations; i++) {
        locations[i] = in.readUTF();
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(numFiles);

    for(Path file : files) {
      out.writeUTF(file.toString());
    }

    for(long offset : offsets) {
      out.writeLong(offset);
    }

    for(long length : lengths) {
      out.writeLong(length);
    }

    if (locations == null || locations.length == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(locations.length);
      for(String location : locations) {
        out.writeUTF(location);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    boolean first = true;
    for(int i = 0; i < files.length; i++) {
      if(first) {
        first = false;
      } else {
        sb.append(", ");
      }

      sb.append(files[i]);
      sb.append(" (offset=").append(offsets[i]);
      sb.append(", end=").append(offsets[i] + lengths[i]);
      sb.append(", length=").append(lengths[i]);
      sb.append(")");
    }
    sb.append("}");
    return sb.toString();
  }

}
