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

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.cloudera.sqoop.lib.SqoopRecord;

/**
 * Reducer for merge tool. Given records tagged as 'old' or 'new', emit
 * a new one if possible; otherwise, an old one.
 */
public class MergeReducer
    extends Reducer<Text, MergeRecord, SqoopRecord, NullWritable> {

  @Override
  public void reduce(Text key, Iterable<MergeRecord> vals, Context c)
      throws IOException, InterruptedException {
    SqoopRecord bestRecord = null;
    try {
      for (MergeRecord val : vals) {
        if (null == bestRecord && !val.isNewRecord()) {
          // Use an old record if we don't have a new record.
          bestRecord = (SqoopRecord) val.getSqoopRecord().clone();
        } else if (val.isNewRecord()) {
          bestRecord = (SqoopRecord) val.getSqoopRecord().clone();
        }
      }
    } catch (CloneNotSupportedException cnse) {
      throw new IOException(cnse);
    }

    if (null != bestRecord) {
      c.write(bestRecord, NullWritable.get());
    }
  }
}

