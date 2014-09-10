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

package org.apache.sqoop.mapreduce.mainframe;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.util.MainframeFTPClientUtils;

/**
 * A RecordReader that returns a record from a mainframe dataset.
 */
public class MainframeDatasetFTPRecordReader <T extends SqoopRecord>
    extends MainframeDatasetRecordReader<T> {
  private FTPClient ftp = null;
  private BufferedReader datasetReader = null;

  private static final Log LOG = LogFactory.getLog(
      MainframeDatasetFTPRecordReader.class.getName());

  @Override
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    super.initialize(inputSplit, taskAttemptContext);

    Configuration conf = getConfiguration();
    ftp = MainframeFTPClientUtils.getFTPConnection(conf);
    if (ftp != null) {
      String dsName
          = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME);
      ftp.changeWorkingDirectory("'" + dsName + "'");
    }
  }

  @Override
  public void close() throws IOException {
    if (datasetReader != null) {
      datasetReader.close();
    }
    if (ftp != null) {
      MainframeFTPClientUtils.closeFTPConnection(ftp);
    }
  }

  protected boolean getNextRecord(T sqoopRecord) throws IOException {
    String line = null;
    try {
      do {
        if (datasetReader == null) {
          String dsName = getNextDataset();
          if (dsName == null) {
            break;
          }
          datasetReader = new BufferedReader(new InputStreamReader(
              ftp.retrieveFileStream(dsName)));
        }
        line = datasetReader.readLine();
        if (line == null) {
          datasetReader.close();
          datasetReader = null;
          if (!ftp.completePendingCommand()) {
            throw new IOException("Failed to complete ftp command.");
          } else {
            LOG.info("Data transfer completed.");
          }
        }
      } while(line == null);
    } catch (IOException ioe) {
      throw new IOException("IOException during data transfer: " +
          ioe.toString());
    }
    if (line != null) {
      convertToSqoopRecord(line, (SqoopRecord)sqoopRecord);
      return true;
    }
    return false;
  }

  private void convertToSqoopRecord(String line,  SqoopRecord sqoopRecord) {
    String fieldName
        = sqoopRecord.getFieldMap().entrySet().iterator().next().getKey();
    sqoopRecord.setField(fieldName, line);
  }
}
