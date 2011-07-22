/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** An {@link org.apache.hadoop.mapred.OutputFormat} for Avro data files. */
public class AvroOutputFormat<T>
  extends FileOutputFormat<AvroWrapper<T>, NullWritable> {

  @Override
  public RecordWriter<AvroWrapper<T>, NullWritable> getRecordWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {

    Schema schema = AvroJob.getMapOutputSchema(context.getConfiguration());

    final DataFileWriter<T> WRITER =
      new DataFileWriter<T>(new GenericDatumWriter<T>());

    Path path = getDefaultWorkFile(context,
        org.apache.avro.mapred.AvroOutputFormat.EXT);
    WRITER.create(schema,
        path.getFileSystem(context.getConfiguration()).create(path));

    return new RecordWriter<AvroWrapper<T>, NullWritable>() {
      @Override
      public void write(AvroWrapper<T> wrapper, NullWritable ignore)
        throws IOException {
        WRITER.append(wrapper.datum());
      }
      @Override
      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        WRITER.close();
      }
    };
  }

}
