/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.job.io;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.utils.ClassUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SqoopWritable implements Configurable, WritableComparable<SqoopWritable> {
  private IntermediateDataFormat<?> dataFormat;
  private Configuration conf;

  public SqoopWritable() {
    this(null);
  }

  public SqoopWritable(IntermediateDataFormat<?> dataFormat) {
    this.dataFormat = dataFormat;
  }

  public void setString(String data) {
    this.dataFormat.setTextData(data);
  }

  public String getString() { return dataFormat.getTextData(); }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(dataFormat.getTextData());
  }

  @Override
  public void readFields(DataInput in) throws IOException { dataFormat.setTextData(in.readUTF()); }

  @Override
  public int compareTo(SqoopWritable o) { return getString().compareTo(o.getString()); }

  @Override
  public String toString() {
    return getString();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    if (dataFormat == null) {
      String intermediateDataFormatName = conf.get(MRJobConstants.INTERMEDIATE_DATA_FORMAT);
      this.dataFormat = (IntermediateDataFormat<?>) ClassUtils.instantiate(intermediateDataFormatName);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
