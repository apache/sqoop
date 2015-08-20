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

package org.apache.sqoop.connector.sftp.sftpclient;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.JSchException;

import org.apache.log4j.Logger;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.sftp.SftpConstants;
import org.apache.sqoop.connector.sftp.SftpConnectorError;
import org.apache.sqoop.etl.io.DataReader;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Properties;

/**
 * Class encapsulating functionality to interact with an SFTP server. This class
 * uses the JSch library to provide the SFTP functionality. See
 * http://www.jcraft.com/jsch/.
 */
public class SftpConnectorClient {

  /**
   * Java secure channel implementation supporting sftp functionality.
   */
  private JSch jsch = null;

  /**
   * jsch session object.
   */
  private Session session = null;

  /**
   * sftp channel object.
   */
  private ChannelSftp channelSftp = null;

  /**
   * Hostname for sftp server.
   */
  private String sftpServer = null;

  /**
   * Port for sftp server.
   */
  private int sftpPort = SftpConstants.DEFAULT_PORT;

  /**
   * Log4j logger.
   */
  private static final Logger LOG = Logger.getLogger(SftpConnectorClient.class);

  /**
   * Default constructor. We'll just initialize the secure channel object here.
   */
  public SftpConnectorClient() {
    jsch = new JSch();
  }

  /**
   * Create a jsch session object, and use it to open a channel to the sftp
   * server.
   *
   * @param host Hostname for SFTP server.
   * @param port Port that SFTP server is running on. 22 by default.
   * @param username Username for logging into server.
   * @param password Password for user login.
   *
   * @exception SqoopException thrown if a JSchException is caught indicating
   * an error during connection.
   */
  public void connect(String host, Integer port, String username, String password)
    throws SqoopException {

    if (port != null) {
      sftpPort = port.intValue();
    }

    try {
      session = jsch.getSession(username, host, sftpPort);
      session.setPassword(password);
      // Disable StrictHostKeyChecking so we don't get an "UnkownHostKey" error:
      Properties fstpProps = new Properties();
      fstpProps.put("StrictHostKeyChecking", "no");
      session.setConfig(fstpProps);
      LOG.info("connecting to " + host + " port=" + sftpPort);
      session.connect();
      LOG.info("successfully connected to " + host);
      Channel channel = session.openChannel("sftp");
      channel.connect();
      channelSftp = (ChannelSftp)channel;
      LOG.info("successfully created sftp channel for " + host);
    } catch (JSchException e) {
      LOG.error("Caught JSchException: " + e.getMessage());
      throw new SqoopException(SftpConnectorError.SFTP_CONNECTOR_0001,
                               e.getMessage(), e);
    }
  }

  /**
   * Upload records to the SFTP server.
   *
   * @param reader a DataReader object containing data passed from source
   * connector.
   * @param path Full path on SFTP server to write files to.
   *
   * @return Number of records written in call to this method.
   *
   * @throws SqoopException thrown if error occurs during interaction with
   * SFTP server.
   */
  public long upload(DataReader reader, String path)
    throws SqoopException {

    long recordsWritten = 0;
    // OutputStream for writing records to SFTP server.
    OutputStream out = null;

    try {
      out = channelSftp.put(path, null, ChannelSftp.OVERWRITE, 0);
      LOG.info("Opened OutputStream to path: " + path);
      String record;
      while ((record = reader.readTextRecord()) != null) {
        out.write(record.getBytes(Charset.forName("UTF-8")));
        out.write(("\n").getBytes(Charset.forName("UTF-8")));
        recordsWritten++;
      }
    } catch (Exception e) {
      LOG.error("Caught Exception writing records to SFTP server: "
                + e.getMessage());
      throw new SqoopException(SftpConnectorError.SFTP_CONNECTOR_0003,
                               e.getMessage(), e);
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } catch (IOException e) {
        LOG.error("Caught IOException closing SFTP output stream: " +
                  e.getMessage());
        throw new SqoopException(SftpConnectorError.SFTP_CONNECTOR_0002,
                               "Caught IOException: " + e.getMessage(), e);
      }
    }

    return recordsWritten;
  }

  /**
   * Disconnect from SFTP server.
   */
  public void disconnect() {
    session.disconnect();
  }
}
