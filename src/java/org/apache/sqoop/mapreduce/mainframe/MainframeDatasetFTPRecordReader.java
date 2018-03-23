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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

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
  private BufferedInputStream inputStream = null;

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
		String dsName = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME);
		String dsType = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE);
		MainframeDatasetPath p = null;
		try {
			p = new MainframeDatasetPath(dsName,conf);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			LOG.error("MainframeDatasetPath helper class incorrectly initialised");
			e.printStackTrace();
		}
		if (dsType != null && p != null) {
			dsName = p.getMainframeDatasetFolder();
		}
		ftp.changeWorkingDirectory("'" + dsName + "'");
    }
  }

  // used for testing
  public void initialize(InputSplit inputSplit,
       TaskAttemptContext taskAttemptContext,
       FTPClient ftpClient, Configuration conf)
    throws IOException, InterruptedException {
    ftp = ftpClient;
    if (ftp != null) {
      String dsName = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME);
      String dsType = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE);
      inputStream = new BufferedInputStream(ftp.retrieveFileStream(dsName));
      MainframeDatasetPath p = null;
      try {
        p = new MainframeDatasetPath(dsName,conf);
      } catch (Exception e) {
        LOG.error(e.getMessage());
        LOG.error("MainframeDatasetPath helper class incorrectly initialised");
        e.printStackTrace();
      }
      if (dsType != null && p != null) {
        dsName = p.getMainframeDatasetFolder();
      }
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
    Configuration conf = getConfiguration();
    if (MainframeConfiguration.MAINFRAME_FTP_TRANSFER_MODE_BINARY.equals(conf.get(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_MODE))) {
      return getNextBinaryRecord(sqoopRecord);
    }
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

  protected boolean getNextBinaryRecord(T sqoopRecord) throws IOException {
    // typical estimated max size for mainframe record
    Configuration conf = getConfiguration();
    int BUFFER_SIZE = conf.getInt(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_BUFFER_SIZE,MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_DEFAULT_BUFFER_SIZE);
    byte[] buf = new byte[BUFFER_SIZE];
    int bytesRead = -1;
    int cumulativeBytesRead = 0;
    String dsName = null;
    try {
      if (inputStream == null) {
        dsName = getNextDataset();
        if (dsName == null) {
          return false;
        }
        inputStream = new BufferedInputStream(ftp.retrieveFileStream(dsName));
        if (inputStream == null) {
          throw new IOException("Failed to retrieve FTP file stream.");
        }
      }
      do {
        bytesRead = inputStream.read(buf,cumulativeBytesRead,BUFFER_SIZE-cumulativeBytesRead);
        if (bytesRead == -1) {
          // EOF
          inputStream.close();
          inputStream = null;
          if (!ftp.completePendingCommand()) {
            throw new IOException("Failed to complete ftp command. FTP Response: "+ftp.getReplyString());
          } else {
            LOG.info("Data transfer completed.");
          }
          return writeBytesToSqoopRecord(buf,cumulativeBytesRead,sqoopRecord);
        }
        cumulativeBytesRead += bytesRead;
        if (cumulativeBytesRead == BUFFER_SIZE) {
          return writeBytesToSqoopRecord(buf,cumulativeBytesRead,sqoopRecord);
        }
      } while (bytesRead != -1);
    } catch (IOException ioe) {
      throw new IOException("IOException during data transfer: " + ioe);
    }
    return false;
  }

  protected Boolean writeBytesToSqoopRecord(byte[] buf, int cumulativeBytesRead, SqoopRecord sqoopRecord) {
    if (cumulativeBytesRead <= 0) {
      return false;
    }
    ByteBuffer buffer = ByteBuffer.allocate(cumulativeBytesRead);
    buffer.put(buf,0,cumulativeBytesRead);
    convertToSqoopRecord(buffer.array(), sqoopRecord);
    return true;
  }

  private void convertToSqoopRecord(String line,  SqoopRecord sqoopRecord) {
    String fieldName
        = sqoopRecord.getFieldMap().entrySet().iterator().next().getKey();
    sqoopRecord.setField(fieldName, line);
  }

  private void convertToSqoopRecord(byte[] buf,  SqoopRecord sqoopRecord) {
    String fieldName
      = sqoopRecord.getFieldMap().entrySet().iterator().next().getKey();
    sqoopRecord.setField(fieldName, buf);
  }
}
