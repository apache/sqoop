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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** An {@link RecordReader} for Avro data files. */
public class AvroRecordReader<T>
  extends RecordReader<AvroWrapper<T>, NullWritable> {

  private FileReader<T> reader;
  private long start;
  private long end;
  private AvroWrapper<T> key;
  private NullWritable value;

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration conf = context.getConfiguration();
    SeekableInput in = new FsInput(split.getPath(), conf);
    DatumReader<T> datumReader = new GenericDatumReader<T>();
    this.reader = DataFileReader.openReader(in, datumReader);
    reader.sync(split.getStart());                    // sync to start
    this.start = reader.tell();
    this.end = split.getStart() + split.getLength();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!reader.hasNext() || reader.pastSync(end)) {
      key = null;
      value = null;
      return false;
    }
    if (key == null) {
      key = new AvroWrapper<T>();
    }
    if (value == null) {
      value = NullWritable.get();
    }
    key.datum(reader.next(key.datum()));
    return true;
  }

  @Override
  public AvroWrapper<T> getCurrentKey() throws IOException,
      InterruptedException {
    return key;
  }

  @Override
  public NullWritable getCurrentValue()
      throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getPos() - start) / (float)(end - start));
    }
  }

  public long getPos() throws IOException {
    return reader.tell();
  }

  @Override
  public void close() throws IOException { reader.close(); }
}

