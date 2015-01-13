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

/**
 * Writable used to load the data to the {@link #Transferable} entity TO
 * It is used for the map output key class and the outputFormat key class in the MR engine
 * For instance: job.setMapOutputKeyClass(..,) and  job.setOutputKeyClass(...);
 */

public class SqoopWritable implements Configurable, WritableComparable<SqoopWritable> {
  private IntermediateDataFormat<?> toIDF;
  private Configuration conf;

  // NOTE: You have to provide an empty default constructor in your key class
  // Hadoop is using reflection and it can not guess any parameters to feed
  public SqoopWritable() {
    this(null);
  }

  public SqoopWritable(IntermediateDataFormat<?> dataFormat) {
    this.toIDF = dataFormat;
  }

  // default/package visibility for testing
  void setString(String data) {
    this.toIDF.setCSVTextData(data);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    //delegate
    toIDF.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    //delegate
    toIDF.read(in);
  }

  @Override
  public int compareTo(SqoopWritable o) {
    return toIDF.compareTo(o.toIDF);
  }

  @Override
  public String toString() {
    return toIDF.toString();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    if (toIDF == null) {
      String toIDFClass = conf.get(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT);
      this.toIDF = (IntermediateDataFormat<?>) ClassUtils.instantiate(toIDFClass);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((toIDF == null) ? 0 : toIDF.hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SqoopWritable other = (SqoopWritable) obj;
    if (toIDF == null) {
      if (other.toIDF != null)
        return false;
    } else if (!toIDF.equals(other.toIDF))
      return false;
    return true;
  }

}
