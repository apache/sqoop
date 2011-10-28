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

package org.apache.sqoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import com.cloudera.sqoop.lib.SqoopRecord;

/**
 * Class that holds a record to be merged. This contains a SqoopRecord which
 * is the "guts" of the item, and a boolean value indicating whether it is a
 * "new" record or an "old" record. In the Reducer, we prefer to emit a new
 * record rather than an old one, if a new one is available.
 */
public class MergeRecord implements Configurable, Writable {
  private SqoopRecord sqoopRecord;
  private boolean isNew;
  private Configuration config;

  /** Construct an empty MergeRecord. */
  public MergeRecord() {
    this.sqoopRecord = null;
    this.isNew = false;
    this.config = new Configuration();
  }

  /**
   * Construct a MergeRecord with all fields initialized.
   */
  public MergeRecord(SqoopRecord sr, boolean recordIsNew) {
    this.sqoopRecord = sr;
    this.isNew = recordIsNew;
    this.config = new Configuration();
  }

  @Override
  /** {@inheritDoc} */
  public void setConf(Configuration conf) {
    this.config = conf;
  }

  @Override
  /** {@inheritDoc} */
  public Configuration getConf() {
    return this.config;
  }

  /** @return true if this record came from the "new" dataset. */
  public boolean isNewRecord() {
    return isNew;
  }

  /**
   * Set the isNew field to 'newVal'.
   */
  public void setNewRecord(boolean newVal) {
    this.isNew = newVal;
  }

  /**
   * @return the underlying SqoopRecord we're shipping.
   */
  public SqoopRecord getSqoopRecord() {
    return this.sqoopRecord;
  }

  /**
   * Set the SqoopRecord instance we should pass from the mapper to the
   * reducer.
   */
  public void setSqoopRecord(SqoopRecord record) {
    this.sqoopRecord = record;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void readFields(DataInput in) throws IOException {
    this.isNew = in.readBoolean();
    String className = Text.readString(in);
    if (null == this.sqoopRecord) {
      // If we haven't already instantiated an inner SqoopRecord, do so here.
      try {
        Class<? extends SqoopRecord> recordClass =
            (Class<? extends SqoopRecord>) config.getClassByName(className);
        this.sqoopRecord = recordClass.newInstance();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    this.sqoopRecord.readFields(in);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(this.isNew);
    Text.writeString(out, this.sqoopRecord.getClass().getName());
    this.sqoopRecord.write(out);
  }

  @Override
  public String toString() {
    return "" + this.sqoopRecord;
  }
}
