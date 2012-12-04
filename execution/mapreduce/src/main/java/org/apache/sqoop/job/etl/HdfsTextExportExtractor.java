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
package org.apache.sqoop.job.etl;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.util.LineReader;
import org.apache.sqoop.common.ImmutableContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.job.MapreduceExecutionError;
import org.apache.sqoop.job.PrefixContext;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.io.DataWriter;

public class HdfsTextExportExtractor extends Extractor {

  private Configuration conf;
  private DataWriter datawriter;

  private final char fieldDelimiter;

  public HdfsTextExportExtractor() {
    fieldDelimiter = Data.DEFAULT_FIELD_DELIMITER;
  }

  @Override
  public void run(ImmutableContext context, Object connectionConfiguration,
      Object jobConfiguration, Partition partition, DataWriter writer) {
    writer.setFieldDelimiter(fieldDelimiter);

    conf = ((PrefixContext)context).getConfiguration();
    datawriter = writer;

    try {
      HdfsExportPartition p = (HdfsExportPartition)partition;
      int numFiles = p.getNumberOfFiles();
      for (int i=0; i<numFiles; i++) {
        extractFile(p.getFile(i), p.getOffset(i), p.getLength(i));
      }
    } catch (IOException e) {
      throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0017, e);
    }
  }

  private void extractFile(Path file, long offset, long length)
      throws IOException {
    long start = offset;
    long end = start + length;

    FileSystem fs = file.getFileSystem(conf);
    FSDataInputStream filestream = fs.open(file);
    CompressionCodec codec = (new CompressionCodecFactory(conf)).getCodec(file);
    LineReader filereader;
    Seekable fileseeker;

    if (codec == null) {
      filestream.seek(start);
      byte[] recordDelimiterBytes = String.valueOf(
          Data.DEFAULT_RECORD_DELIMITER).getBytes(
              Charset.forName(Data.CHARSET_NAME));
      filereader = new LineReader(filestream, conf,
          recordDelimiterBytes);
      fileseeker = filestream;

    } else if (codec instanceof SplittableCompressionCodec) {
      SplitCompressionInputStream compressionstream =
          ((SplittableCompressionCodec)codec).createInputStream(
              filestream, codec.createDecompressor(), start, end,
              SplittableCompressionCodec.READ_MODE.BYBLOCK);
      byte[] recordDelimiterBytes = String.valueOf(
          Data.DEFAULT_RECORD_DELIMITER).getBytes(
              Charset.forName(Data.CHARSET_NAME));
      filereader = new LineReader(compressionstream,
          conf, recordDelimiterBytes);
      fileseeker = compressionstream;

      start = compressionstream.getAdjustedStart();
      end = compressionstream.getAdjustedEnd();

    } else {
      byte[] recordDelimiterBytes = String.valueOf(
          Data.DEFAULT_RECORD_DELIMITER).getBytes(
              Charset.forName(Data.CHARSET_NAME));
      filereader = new LineReader(
          codec.createInputStream(filestream, codec.createDecompressor()),
          conf, recordDelimiterBytes);
      fileseeker = filestream;
    }

    if (start != 0) {
      // always throw away first record because
      // one extra line is read in previous split
      start += filereader.readLine(new Text(), 0);
    }

    Text line = new Text();
    int size;
    while (fileseeker.getPos() <= end) {
      size = filereader.readLine(line, Integer.MAX_VALUE);
      if (size == 0) {
        break;
      }

      datawriter.writeCsvRecord(line.toString());
    }
  }

  @Override
  public long getRowsRead() {
    // TODO need to return the rows read
    return 0;
  }

}
