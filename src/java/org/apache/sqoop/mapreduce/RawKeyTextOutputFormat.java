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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.*;

/**
 * An {@link OutputFormat} that writes plain text files.
 * Only writes the key. Does not write any delimiter/newline after the key.
 */
public class RawKeyTextOutputFormat<K, V> extends FileOutputFormat<K, V> {

  protected FSDataOutputStream getFSDataOutputStream(TaskAttemptContext context, String ext) throws IOException {
    Configuration conf = context.getConfiguration();
    Path file = getDefaultWorkFile(context, ext);
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);
    return fileOut;
  }

  protected DataOutputStream getOutputStream(TaskAttemptContext context) throws IOException {
    boolean isCompressed = getCompressOutput(context);
    Configuration conf = context.getConfiguration();
    String ext = "";
    CompressionCodec codec = null;

    if (isCompressed) {
      // create the named codec
      Class<? extends CompressionCodec> codecClass =
        getOutputCompressorClass(context, GzipCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);

      ext = codec.getDefaultExtension();
    }

    FSDataOutputStream fileOut = getFSDataOutputStream(context,ext);
    DataOutputStream ostream = fileOut;

    if (isCompressed) {
      ostream = new DataOutputStream(codec.createOutputStream(fileOut));
    }
    return ostream;
  }

  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    DataOutputStream ostream = getOutputStream(context);
    return new KeyRecordWriters.RawKeyRecordWriter<K, V>(ostream);
  }
}