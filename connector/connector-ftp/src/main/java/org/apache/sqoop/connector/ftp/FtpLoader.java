/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.connector.ftp;

import org.apache.sqoop.connector.ftp.configuration.LinkConfiguration;
import org.apache.sqoop.connector.ftp.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.ftp.ftpclient.FtpConnectorClient;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;

import java.util.UUID;

/**
 * Class to receive data from a From instance and load to a To instance.
 */
public class FtpLoader extends Loader<LinkConfiguration, ToJobConfiguration> {

  /**
   * Number of records written by last call to load() method.
   */
  private long rowsWritten = 0;

  /**
   * Load data to target directory on FTP server.
   *
   * @param context Loader context object
   * @param linkConfiguration Link configuration
   * @param toJobConfig Job configuration
   * @throws Exception Re-thrown from FTP client code.
   */
  @Override
  public void load(LoaderContext context, LinkConfiguration linkConfiguration,
                   ToJobConfiguration toJobConfig) throws Exception {
    DataReader reader = context.getDataReader();
    String outputDir = toJobConfig.toJobConfig.outputDirectory;
    // Create a unique filename for writing records, since this method will
    // likely get called multiple times for a single source file/dataset:
    String path = outputDir + "/" + UUID.randomUUID() + ".txt";

    FtpConnectorClient client =
      new FtpConnectorClient(linkConfiguration.linkConfig.server,
                             linkConfiguration.linkConfig.port);
    client.connect(linkConfiguration.linkConfig.username,
                   linkConfiguration.linkConfig.password);
    rowsWritten = client.uploadStream(reader, path);
    client.disconnect();
  }

  /**
   * Return the number of rows witten by the last call to load() method.
   *
   * @return Number of rows written by call to loader.
   */
  @Override
  public long getRowsWritten() {
    return rowsWritten;
  }
}
