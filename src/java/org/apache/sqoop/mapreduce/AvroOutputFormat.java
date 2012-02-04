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
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL;
import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;
import static org.apache.avro.mapred.AvroOutputFormat.DEFAULT_DEFLATE_LEVEL;
import static org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY;
import static org.apache.avro.mapred.AvroOutputFormat.EXT;
import static org.apache.avro.mapred.AvroOutputFormat.SYNC_INTERVAL_KEY;

/**
 * An {@link org.apache.hadoop.mapred.OutputFormat} for Avro data files.
 * <p/>
 * Note: This class is copied from the Avro project in version 1.5.4 and
 * adapted here to work with the "new" MapReduce API that's required in Sqoop.
 */
public class AvroOutputFormat<T>
  extends FileOutputFormat<AvroWrapper<T>, NullWritable> {

  static <T> void configureDataFileWriter(DataFileWriter<T> writer,
    TaskAttemptContext context) throws UnsupportedEncodingException {
    if (FileOutputFormat.getCompressOutput(context)) {
      int level = context.getConfiguration()
        .getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
      String codecName = context.getConfiguration()
        .get(org.apache.avro.mapred.AvroJob.OUTPUT_CODEC, DEFLATE_CODEC);
      CodecFactory factory =
        codecName.equals(DEFLATE_CODEC) ? CodecFactory.deflateCodec(level)
          : CodecFactory.fromString(codecName);
      writer.setCodec(factory);
    }

    writer.setSyncInterval(context.getConfiguration()
      .getInt(SYNC_INTERVAL_KEY, DEFAULT_SYNC_INTERVAL));

    // copy metadata from job
    for (Map.Entry<String, String> e : context.getConfiguration()) {
      if (e.getKey().startsWith(org.apache.avro.mapred.AvroJob.TEXT_PREFIX)) {
        writer.setMeta(e.getKey()
          .substring(org.apache.avro.mapred.AvroJob.TEXT_PREFIX.length()),
          e.getValue());
      }
      if (e.getKey().startsWith(org.apache.avro.mapred.AvroJob.BINARY_PREFIX)) {
        writer.setMeta(e.getKey()
          .substring(org.apache.avro.mapred.AvroJob.BINARY_PREFIX.length()),
          URLDecoder.decode(e.getValue(), "ISO-8859-1").getBytes("ISO-8859-1"));
      }
    }
  }

  @Override
  public RecordWriter<AvroWrapper<T>, NullWritable> getRecordWriter(
    TaskAttemptContext context) throws IOException, InterruptedException {

    boolean isMapOnly = context.getNumReduceTasks() == 0;
    Schema schema =
      isMapOnly ? AvroJob.getMapOutputSchema(context.getConfiguration())
        : AvroJob.getOutputSchema(context.getConfiguration());

    final DataFileWriter<T> WRITER =
      new DataFileWriter<T>(new ReflectDatumWriter<T>());

    configureDataFileWriter(WRITER, context);

    Path path = getDefaultWorkFile(context, EXT);
    WRITER.create(schema,
      path.getFileSystem(context.getConfiguration()).create(path));

    return new RecordWriter<AvroWrapper<T>, NullWritable>() {
      @Override
      public void write(AvroWrapper<T> wrapper, NullWritable ignore)
        throws IOException {
        WRITER.append(wrapper.datum());
      }

      @Override
      public void close(TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
        WRITER.close();
      }
    };
  }

}
