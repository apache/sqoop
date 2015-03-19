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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.apache.sqoop.error.code.HdfsConnectorError;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;

import java.io.IOException;
import java.util.UUID;

public class HdfsToInitializer extends Initializer<LinkConfiguration, ToJobConfiguration> {

  private static final Logger LOG = Logger.getLogger(HdfsToInitializer.class);

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(InitializerContext context, LinkConfiguration linkConfig, ToJobConfiguration jobConfig) {
    assert jobConfig != null;
    assert linkConfig != null;
    assert jobConfig.toJobConfig != null;
    assert jobConfig.toJobConfig.outputDirectory != null;

    Configuration configuration = HdfsUtils.createConfiguration(linkConfig);
    HdfsUtils.configurationToContext(configuration, context.getContext());
    boolean appendMode = Boolean.TRUE.equals(jobConfig.toJobConfig.appendMode);

    // Verification that given HDFS directory either don't exists or is empty
    try {
      FileSystem fs = FileSystem.get(configuration);
      Path path = new Path(jobConfig.toJobConfig.outputDirectory);

      if(fs.exists(path)) {
        if(fs.isFile(path)) {
          throw new SqoopException(HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0007, "Output directory already exists and is a file");
        }

        if(fs.isDirectory(path) && !appendMode) {
          FileStatus[] fileStatuses = fs.listStatus(path);
          if(fileStatuses.length != 0) {
            throw new SqoopException(HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0007, "Output directory is not empty");
          }
        }
      }
    } catch (IOException e) {
      throw new SqoopException(HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0007, "Unexpected exception", e);
    }

    // Building working directory
    String workingDirectory = jobConfig.toJobConfig.outputDirectory + "/." + UUID.randomUUID();
    LOG.info("Using working directory: " + workingDirectory);
    context.getContext().setString(HdfsConstants.WORK_DIRECTORY, workingDirectory);
  }
}
