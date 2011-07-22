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

package org.apache.hadoop.sqoop.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;
import org.apache.hadoop.sqoop.SqoopOptions;
import org.apache.hadoop.sqoop.io.SplittableBufferedWriter;
import org.apache.hadoop.sqoop.lib.FieldFormatter;
import org.apache.hadoop.sqoop.lib.RecordParser;
import org.apache.hadoop.sqoop.util.AsyncSink;
import org.apache.hadoop.sqoop.util.DirectImportUtils;
import org.apache.hadoop.sqoop.util.ErrorableAsyncSink;
import org.apache.hadoop.sqoop.util.ErrorableThread;
import org.apache.hadoop.sqoop.util.ImportException;
import org.apache.hadoop.sqoop.util.JdbcUrl;
import org.apache.hadoop.sqoop.util.LoggingAsyncSink;
import org.apache.hadoop.sqoop.util.PerfCounters;

/**
 * InputFormat designed to take data-driven splits and feed them to a mysqldump
 * invocation running in the mapper.
 *
 * The key emitted by this mapper is a WHERE clause to use in the command
 * to mysqldump.
 */
public class MySQLDumpInputFormat extends DataDrivenDBInputFormat {

  public static final Log LOG = LogFactory.getLog(
      MySQLDumpInputFormat.class.getName());

  /**
   * A RecordReader that just takes the WHERE conditions from the DBInputSplit
   * and relates them to the mapper as a single input record.
   */
  public static class MySQLDumpRecordReader
      extends RecordReader<String, NullWritable> {

    private InputSplit split;

    private boolean delivered;
    private String clause;

    public MySQLDumpRecordReader(InputSplit split) {
      initialize(split, null);
    }

    @Override
    public boolean nextKeyValue() {
      boolean hasNext = !delivered;
      delivered = true;
      return hasNext;
    }

    @Override
    public String getCurrentKey() {
      return clause;
    }

    @Override
    public NullWritable getCurrentValue() {
      return NullWritable.get();
    }

    @Override
    public void close() {
    }

    @Override
    public float getProgress() {
      return delivered ? 1.0f : 0.0f;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
      this.split = split;
      DataDrivenDBInputFormat.DataDrivenDBInputSplit dbSplit =
          (DataDrivenDBInputFormat.DataDrivenDBInputSplit) split;

      this.clause = "(" + dbSplit.getLowerClause() + ") AND ("
          + dbSplit.getUpperClause() + ")";
    }
  }

  public RecordReader<String, NullWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) {
    return new MySQLDumpRecordReader(split);
  }

}

