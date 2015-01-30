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
package org.apache.sqoop.job.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.MRExecutionError;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.utils.ClassUtils;

/**
 * An input split to be read.
 */
public class SqoopSplit extends InputSplit implements Writable {

  private Partition partition;

  public void setPartition(Partition partition) {
    this.partition = partition;
  }

  public Partition getPartition() {
    return partition;
  }

  @Override
  public long getLength() throws IOException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[] {};
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // read Partition class name
    String className = in.readUTF();
    // instantiate Partition object
    Class<?> clz = ClassUtils.loadClass(className);
    if (clz == null) {
      throw new SqoopException(MRExecutionError.MAPRED_EXEC_0009, className);
    }
    try {
      partition = (Partition) clz.newInstance();
    } catch (Exception e) {
      throw new SqoopException(MRExecutionError.MAPRED_EXEC_0010, className, e);
    }
    // read Partition object content
    partition.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // write Partition class name
    out.writeUTF(partition.getClass().getName());
    // write Partition object content
    partition.write(out);
  }

}
