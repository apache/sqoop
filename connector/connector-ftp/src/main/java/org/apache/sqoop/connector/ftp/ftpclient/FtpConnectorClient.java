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

package org.apache.sqoop.connector.ftp.ftpclient;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ftp.FtpConstants;
import org.apache.sqoop.connector.ftp.FtpConnectorError;
import org.apache.sqoop.etl.io.DataReader;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.UUID;

/**
 * Class encapsulating functionality to interact with an FTP server. This class
 * uses the Apache Commons Net libraries to provide the FTP functionality. See
 * http://commons.apache.org/proper/commons-net/.
 */
public class FtpConnectorClient {

  /**
   * Apache Commons Net FTP client.
   */
  private FTPClient ftpClient = null;

  /**
   * Hostname for FTP server.
   */
  private String ftpServer = null;

  /**
   * Port for FTP server.
   */
  private int ftpPort = FtpConstants.DEFAULT_PORT;

  private static final Logger LOG = Logger.getLogger(FtpConnectorClient.class);

  /**
   * Constructor to initialize client.
   *
   * @param server Hostname of FTP server.
   * @param port Port number of FTP server. Pass in null to use default port
   * of 21.
   */
  public FtpConnectorClient(String server, Integer port) {
    ftpClient = new FTPClient();
    ftpServer = server;
    if (port != null) {
      ftpPort = port.intValue();
    }
  }

  /**
   * Connect to the FTP server.
   *
   * @param username Username for server login.
   * @param pass Password for server login.
   *
   * @throws SqoopException Thrown if an error occurs while connecting to server.
   */
  public void connect(String username, String pass)
    throws SqoopException {

    try {
      ftpClient.connect(ftpServer, ftpPort);
      LOG.info(getServerReplyAsString());
      int replyCode = ftpClient.getReplyCode();
      if (!FTPReply.isPositiveCompletion(replyCode)) {
        ftpClient.disconnect();
        LOG.error("Operation failed. Server reply code: " + replyCode);
        throw new SqoopException(FtpConnectorError.FTP_CONNECTOR_0001,
                                 "Server reply code=" + replyCode);
      }

      boolean success = ftpClient.login(username, pass);
      LOG.info(getServerReplyAsString());
      if (!success) {
        ftpClient.disconnect();
        LOG.error("Could not login to the server" + ftpServer);
        throw new SqoopException(FtpConnectorError.FTP_CONNECTOR_0001);
      } else {
        LOG.info("logged into " + ftpServer);
      }
    } catch (IOException e) {
      LOG.error("Caught IOException connecting to FTP server: " +
                e.getMessage());
      throw new SqoopException(FtpConnectorError.FTP_CONNECTOR_0001,
                               "Caught IOException: " + e.getMessage(), e);
    }
  }

  /**
   * Log out and disconnect from FTP server.
   *
   * @throws SqoopException Thrown if an error occurs while disconnecting from
   * server.
   */
  public void disconnect()
    throws SqoopException {
    try {
      ftpClient.logout();
      ftpClient.disconnect();
    } catch (IOException e) {
      LOG.error("Caught IOException disconnecting from FTP server: " +
                e.getMessage());
      throw new SqoopException(FtpConnectorError.FTP_CONNECTOR_0002,
                               "Caught IOException: " + e.getMessage(), e);
    }
  }

  /**
   * Stream records to a file on the FTP server.
   *
   * @param reader a DataReader object containing data passed from source
   * connector.
   * @param path directory on FTP server to write files to.
   *
   * @return Number of records written in call to this method.
   *
   * @throws SqoopException thrown if error occurs during interaction with
   * FTP server.
   * @throws Exception thrown if error occurs in DataReader.
   */
  public long uploadStream(DataReader reader, String path)
    throws SqoopException, Exception {

    OutputStream output = null;

    long recordsWritten = 0;

    try {
      output = ftpClient.storeFileStream(path);
      if (!FTPReply.isPositivePreliminary(ftpClient.getReplyCode())) {
        LOG.error("File transfer failed, server reply=" +
                  getServerReplyAsString());
        throw new SqoopException(FtpConnectorError.FTP_CONNECTOR_0003,
                                 getServerReplyAsString());
      } else {
        String record;
        while ((record = reader.readTextRecord()) != null) {
          LOG.info("Writing record to FTP server:" + record);
          output.write(record.getBytes(Charset.forName("UTF-8")));
          output.write(("\n").getBytes(Charset.forName("UTF-8")));
          recordsWritten++;
        }

        output.close();
        if (!ftpClient.completePendingCommand()) {
          LOG.error("File transfer failed, server reply=" +
                    getServerReplyAsString());
          throw new SqoopException(FtpConnectorError.FTP_CONNECTOR_0003,
                                   getServerReplyAsString());
        }
      }
    } catch (IOException e) {
      LOG.error("Caught IOException: " + e.getMessage());
      throw new SqoopException(FtpConnectorError.FTP_CONNECTOR_0003,
                               "Caught IOException: " + e.getMessage(), e);
    } finally {
      try {
        if (output != null) {
          output.close();
        }
      } catch (IOException e) {
        LOG.error("Caught IOException closing FTP output stream: " +
                  e.getMessage());
        // Throw this in case there was an underlying issue with closing the stream:
        throw new SqoopException(FtpConnectorError.FTP_CONNECTOR_0003,
                                 "Caught IOException closing output stream to FTP server: "
                                 + e.getMessage(), e);
      }
    }

    return recordsWritten;
 }

  /**
   * Turn a collection of reply strings from the FTP server into a single string.
   * @return String containing a concatenation of replies from the server.
   */
  private String getServerReplyAsString() {
    String[] replies = ftpClient.getReplyStrings();
    return StringUtils.join(replies);
  }
}
