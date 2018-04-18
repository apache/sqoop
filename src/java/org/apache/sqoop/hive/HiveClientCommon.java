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

package org.apache.sqoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.io.CodecMap;

import java.io.IOException;

/**
 * Class containing the common logic for different HiveClient implementations.
 */
public class HiveClientCommon {

  public static final Log LOG = LogFactory.getLog(HiveClientCommon.class.getName());

  /**
   * If we used a MapReduce-based upload of the data, remove the _logs dir
   * from where we put it, before running Hive LOAD DATA INPATH.
   */
  public void removeTempLogs(Configuration configuration, Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(configuration);
    Path logsPath = new Path(tablePath, "_logs");
    if (fs.exists(logsPath)) {
      LOG.info("Removing temporary files from import process: " + logsPath);
      if (!fs.delete(logsPath, true)) {
        LOG.warn("Could not delete temporary files; "
            + "continuing with import, but it may fail.");
      }
    }
  }

  /**
   * Clean up after successful HIVE import.
   *
   * @param outputPath path to the output directory
   * @throws IOException
   */
  public void cleanUp(Configuration configuration, Path outputPath) throws IOException {
    FileSystem fs = outputPath.getFileSystem(configuration);

    // HIVE is not always removing input directory after LOAD DATA statement
    // (which is our export directory). We're removing export directory in case
    // that is blank for case that user wants to periodically populate HIVE
    // table (for example with --hive-overwrite).
    try {
      if (outputPath != null && fs.exists(outputPath)) {
        FileStatus[] statuses = fs.listStatus(outputPath);
        if (statuses.length == 0) {
          LOG.info("Export directory is empty, removing it.");
          fs.delete(outputPath, true);
        } else if (statuses.length == 1 && statuses[0].getPath().getName().equals(FileOutputCommitter.SUCCEEDED_FILE_NAME)) {
          LOG.info("Export directory is contains the _SUCCESS file only, removing the directory.");
          fs.delete(outputPath, true);
        } else {
          LOG.info("Export directory is not empty, keeping it.");
        }
      }
    } catch(IOException e) {
      LOG.error("Issue with cleaning (safe to ignore)", e);
    }
  }

  public void indexLzoFiles(SqoopOptions sqoopOptions, Path finalPath) throws IOException {
    String codec = sqoopOptions.getCompressionCodec();
    if (codec != null && (codec.equals(CodecMap.LZOP)
        || codec.equals(CodecMap.getCodecClassName(CodecMap.LZOP)))) {
      try {
        Tool tool = ReflectionUtils.newInstance(Class.
            forName("com.hadoop.compression.lzo.DistributedLzoIndexer").
            asSubclass(Tool.class), sqoopOptions.getConf());
        ToolRunner.run(sqoopOptions.getConf(), tool,
            new String[]{finalPath.toString()});
      } catch (Exception ex) {
        LOG.error("Error indexing lzo files", ex);
        throw new IOException("Error indexing lzo files", ex);
      }
    }
  }

}
