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
package org.apache.sqoop.connector.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.common.SqoopIDFUtils;
import org.apache.sqoop.connector.hdfs.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.error.code.HdfsConnectorError;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.schema.Schema;

/**
 * Extract from HDFS.
 * Default field delimiter of a record is comma.
 */
public class HdfsExtractor extends Extractor<LinkConfiguration, FromJobConfiguration, HdfsPartition> {

  public static final Logger LOG = Logger.getLogger(HdfsExtractor.class);

  private Configuration conf = new Configuration();
  private DataWriter dataWriter;
  private Schema schema;
  private long rowsRead = 0;

  @Override
  public void extract(ExtractorContext context, LinkConfiguration linkConfiguration, FromJobConfiguration jobConfiguration, HdfsPartition partition) {
    HdfsUtils.contextToConfiguration(context.getContext(), conf);
    dataWriter = context.getDataWriter();
    schema = context.getSchema();

    try {
      HdfsPartition p = partition;
      LOG.info("Working on partition: " + p);
      int numFiles = p.getNumberOfFiles();
      for (int i = 0; i < numFiles; i++) {
        extractFile(linkConfiguration, jobConfiguration, p.getFile(i), p.getOffset(i), p.getLength(i));
      }
    } catch (IOException e) {
      throw new SqoopException(HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0001, e);
    }
  }

  private void extractFile(LinkConfiguration linkConfiguration,
                           FromJobConfiguration fromJobCOnfiguration,
                           Path file, long start, long length)
      throws IOException {
    long end = start + length;
    LOG.info("Extracting file " + file);
    LOG.info("\t from offset " + start);
    LOG.info("\t to offset " + end);
    LOG.info("\t of length " + length);
    if(isSequenceFile(file)) {
      extractSequenceFile(linkConfiguration, fromJobCOnfiguration, file, start, length);
    } else {
      extractTextFile(linkConfiguration, fromJobCOnfiguration, file, start, length);
    }
  }

  /**
   * Extracts Sequence file
   * @param file
   * @param start
   * @param length
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  private void extractSequenceFile(LinkConfiguration linkConfiguration,
                                   FromJobConfiguration fromJobConfiguration,
                                   Path file, long start, long length)
      throws IOException {
    LOG.info("Extracting sequence file");
    long end = start + length;
    SequenceFile.Reader filereader = new SequenceFile.Reader(
        file.getFileSystem(conf), file, conf);

    if (start > filereader.getPosition()) {
      filereader.sync(start); // sync to start
    }

    Text line = new Text();
    boolean hasNext = filereader.next(line);
    while (hasNext) {
      rowsRead++;
      if (HdfsUtils.hasCustomFormat(linkConfiguration, fromJobConfiguration)) {
        Object[] data = SqoopIDFUtils.fromCSV(line.toString(), schema);
        dataWriter.writeArrayRecord(HdfsUtils.formatRecord(linkConfiguration, fromJobConfiguration, data));
      } else {
        dataWriter.writeStringRecord(line.toString());
      }
      line = new Text();
      hasNext = filereader.next(line);
      if (filereader.getPosition() >= end && filereader.syncSeen()) {
        break;
      }
    }
    filereader.close();
  }

  /**
   * Extracts Text file
   * @param file
   * @param start
   * @param length
   * @throws IOException
   */
  @SuppressWarnings("resource")
  private void extractTextFile(LinkConfiguration linkConfiguration,
                               FromJobConfiguration fromJobConfiguration,
                               Path file, long start, long length)
      throws IOException {
    LOG.info("Extracting text file");
    long end = start + length;
    FileSystem fs = file.getFileSystem(conf);
    FSDataInputStream filestream = fs.open(file);
    CompressionCodec codec = (new CompressionCodecFactory(conf)).getCodec(file);
    LineReader filereader;
    Seekable fileseeker = filestream;

    // Hadoop 1.0 does not have support for custom record delimiter and thus
    // we
    // are supporting only default one.
    // We might add another "else if" case for SplittableCompressionCodec once
    // we drop support for Hadoop 1.0.
    if (codec == null) {
      filestream.seek(start);
      filereader = new LineReader(filestream);
    } else {
      filereader = new LineReader(codec.createInputStream(filestream,
          codec.createDecompressor()), conf);
      fileseeker = filestream;
    }
    if (start != 0) {
      // always throw away first record because
      // one extra line is read in previous split
      start += filereader.readLine(new Text(), 0);
    }
    int size;
    LOG.info("Start position: " + String.valueOf(start));
    long next = start;
    while (next <= end) {
      Text line = new Text();
      size = filereader.readLine(line, Integer.MAX_VALUE);
      if (size == 0) {
        break;
      }
      if (codec == null) {
        next += size;
      } else {
        next = fileseeker.getPos();
      }
      rowsRead++;
      if (HdfsUtils.hasCustomFormat(linkConfiguration, fromJobConfiguration)) {
        Object[] data = SqoopIDFUtils.fromCSV(line.toString(), schema);
        dataWriter.writeArrayRecord(HdfsUtils.formatRecord(linkConfiguration, fromJobConfiguration, data));
      } else {
        dataWriter.writeStringRecord(line.toString());
      }
    }
    LOG.info("Extracting ended on position: " + fileseeker.getPos());
    filestream.close();
  }

  @Override
  public long getRowsRead() {
    return rowsRead;
  }

  /**
   * Returns true if given file is sequence
   * @param file
   * @return boolean
   */
  @SuppressWarnings("deprecation")
  private boolean isSequenceFile(Path file) {
    SequenceFile.Reader filereader = null;
    try {
      filereader = new SequenceFile.Reader(file.getFileSystem(conf), file, conf);
      filereader.close();
    } catch (IOException e) {
      return false;
    }
    return true;
  }


}
