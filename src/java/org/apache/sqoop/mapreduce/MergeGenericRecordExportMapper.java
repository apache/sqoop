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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.cloudera.sqoop.lib.SqoopRecord;

public class MergeGenericRecordExportMapper<K, V>
    extends AutoProgressMapper<K, V, Text, MergeRecord> {

  protected MapWritable columnTypes = new MapWritable();
  private String keyColName;
  private boolean isNewDatasetSplit;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    keyColName = conf.get(MergeJob.MERGE_KEY_COL_KEY);

    InputSplit inputSplit = context.getInputSplit();
    FileSplit fileSplit = (FileSplit) inputSplit;
    Path splitPath = fileSplit.getPath();

    if (splitPath.toString().startsWith(conf.get(MergeJob.MERGE_NEW_PATH_KEY))) {
      this.isNewDatasetSplit = true;
    } else if (splitPath.toString().startsWith(conf.get(MergeJob.MERGE_OLD_PATH_KEY))) {
      this.isNewDatasetSplit = false;
    } else {
      throw new IOException(
          "File " + splitPath + " is not under new path " + conf.get(MergeJob.MERGE_NEW_PATH_KEY)
              + " or old path " + conf.get(MergeJob.MERGE_OLD_PATH_KEY));
    }
    super.setup(context);
  }

  protected void processRecord(SqoopRecord sqoopRecord, Context context) throws IOException, InterruptedException {
    MergeRecord mergeRecord = new MergeRecord(sqoopRecord, isNewDatasetSplit);
    Map<String, Object> fieldMap = sqoopRecord.getFieldMap();
    if (null == fieldMap) {
      throw new IOException("No field map in record " + sqoopRecord);
    }
    Object keyObj = fieldMap.get(keyColName);
    if (null == keyObj) {
      throw new IOException(
          "Cannot join values on null key. " + "Did you specify a key column that exists?");
    } else {
      context.write(new Text(keyObj.toString()), mergeRecord);
    }
  }
}