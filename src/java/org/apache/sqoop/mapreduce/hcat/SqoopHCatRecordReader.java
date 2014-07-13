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

package org.apache.sqoop.mapreduce.hcat;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.data.HCatRecord;

/**
 * A Record Reader that can combine underlying splits.
 */
public class SqoopHCatRecordReader extends
  RecordReader<WritableComparable, HCatRecord> {
  private final SqoopHCatExportFormat hCatExportFormat;
  private SqoopHCatInputSplit hCatSplit;
  private TaskAttemptContext context;
  private int subIndex;
  private long progress;

  private RecordReader<WritableComparable, HCatRecord> curReader;

  public static final Log LOG = LogFactory
    .getLog(SqoopHCatRecordReader.class.getName());

  public SqoopHCatRecordReader(final InputSplit split,
    final TaskAttemptContext context, final SqoopHCatExportFormat inputFormat)
    throws IOException {
    this.hCatSplit = (SqoopHCatInputSplit) split;
    this.context = context;
    this.subIndex = 0;
    this.curReader = null;
    this.progress = 0L;
    this.hCatExportFormat = inputFormat;

    initNextRecordReader();
  }

  @Override
  public void initialize(final InputSplit split,
    final TaskAttemptContext ctxt)
    throws IOException, InterruptedException {
    this.hCatSplit = (SqoopHCatInputSplit) split;
    this.context = ctxt;

    if (null != this.curReader) {
      this.curReader.initialize(((SqoopHCatInputSplit) split)
        .get(0), context);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    while (this.curReader == null || !this.curReader.nextKeyValue()) {
      if (!initNextRecordReader()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public WritableComparable getCurrentKey() throws IOException,
    InterruptedException {
    return this.curReader.getCurrentKey();
  }

  @Override
  public HCatRecord getCurrentValue() throws IOException, InterruptedException {
    return this.curReader.getCurrentValue();
  }

  @Override
  public void close() throws IOException {
    if (this.curReader != null) {
      this.curReader.close();
      this.curReader = null;
    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    long subprogress = 0L;
    if (null != this.curReader) {
      subprogress = (long) (this.curReader.getProgress()
        * this.hCatSplit.get(this.subIndex - 1).getLength());
    }
    // Indicate the total processed count.
    return Math.min(1.0F, (this.progress + subprogress)
      / (float) this.hCatSplit.getLength());
  }

  protected boolean initNextRecordReader() throws IOException {
    if (this.curReader != null) {
      // close current record reader if open
      this.curReader.close();
      this.curReader = null;
      if (this.subIndex > 0) {
        this.progress +=
          this.hCatSplit.get(this.subIndex - 1).getLength();
      }
      LOG.debug("Closed current reader.  Current progress = " + progress);
    }

    if (this.subIndex == this.hCatSplit.length()) {
      LOG.debug("Done with all splits");
      return false;
    }

    try {
      // get a record reader for the subsplit-index chunk

      this.curReader = this.hCatExportFormat.createHCatRecordReader(
        this.hCatSplit.get(this.subIndex), this.context);

      LOG.debug("Created a HCatRecordReader for split " + subIndex);
      // initialize() for the first RecordReader will be called by MapTask;
      // we're responsible for initializing subsequent RecordReaders.
      if (this.subIndex > 0) {
        this.curReader.initialize(this.hCatSplit.get(this.subIndex),
          this.context);
        LOG.info("Initialized reader with current split");
      }
    } catch (Exception e) {
      throw new IOException("Error initializing HCat record reader", e);
    }
    LOG.debug("Created record reader for subsplit " + subIndex);
    ++this.subIndex;
    return true;
  }
}

