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
package org.apache.sqoop.connector.ftp.configuration;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ftp.ftpclient.FtpConnectorClient;
import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.validators.AbstractValidator;
import org.apache.sqoop.validation.validators.NotEmpty;

/**
 * Attributes for FTP connector link configuration.
 */
@ConfigClass(validators = {@Validator(LinkConfig.ConfigValidator.class)})
public class LinkConfig {

  /**
   * FTP server hostname.
   */
  @Input(size = 256, validators = {@Validator(NotEmpty.class)})
  public String server;

  /**
   * FTP server port. Default is port 21.
   */
  @Input
  public Integer port;

  /**
   * Username for server login.
   */
  @Input(size = 256, validators = {@Validator(NotEmpty.class)})
  public String username;

  /**
   * Password for server login.
   */
  @Input(size = 256, sensitive = true)
  public String password;

  /**
   * Validate that we can log into the server using the supplied credentials.
   */
  public static class ConfigValidator extends AbstractValidator<LinkConfig> {
    @Override
    public void validate(LinkConfig linkConfig) {
      try {
        FtpConnectorClient client =
          new FtpConnectorClient(linkConfig.server, linkConfig.port);
        client.connect(linkConfig.username, linkConfig.password);
        client.disconnect();
      } catch (SqoopException e) {
        addMessage(Status.WARNING, "Can't connect to the FTP server " +
                   linkConfig.server + " error is " + e.getMessage());
      }
    }
  }
}
