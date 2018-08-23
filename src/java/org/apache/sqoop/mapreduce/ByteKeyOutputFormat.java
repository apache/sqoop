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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An {@link OutputFormat} that writes binary files.
 * Only writes the key. Does not write any delimiter/newline after the key.
 */
public class ByteKeyOutputFormat<K, V> extends RawKeyTextOutputFormat<K, V> {

  // currently don't support compression
  private static final String FILE_EXTENSION = "";

  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    DataOutputStream ostream = getFSDataOutputStream(context,FILE_EXTENSION);
    return new KeyRecordWriters.BinaryKeyRecordWriter<K,V>(ostream);
  }

}
