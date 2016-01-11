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
package org.apache.sqoop.test.kdc;

import java.net.URL;

import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.sqoop.client.SqoopClient;

/**
 * Kdc runner for testing purpose.
 *
 * Runner provides methods for bootstrapping and using Kdc. This
 * abstract implementation is agnostic about in what mode Kdc is running.
 * Each mode will have it's own concrete implementation (for example
 * MiniKdc or Real existing kdc).
 */
public abstract class KdcRunner {

  /**
   * Temporary path that can be used as a root for other directories of kdc.
   */
  private String temporaryPath;

  /**
   * Start kdc.
   *
   * @throws Exception
   */
  public abstract void start() throws Exception;

  /**
   * Stop kdc.
   *
   * @throws Exception
   */
  public abstract void stop() throws Exception;

  /**
   * Trigger client to do kerberos authentication with sqoop server, a delegation token will
   * be generated and subsequent requests don't need to do kerberos authentication any more.
   */
  public abstract void authenticateWithSqoopServer(final SqoopClient client) throws Exception;

  /**
   * Trigger client to do kerberos authentication with sqoop server, a delegation token will
   * be generated and subsequent requests which uses this token don't need to do kerberos
   * authentication any more.
   */
  public abstract void authenticateWithSqoopServer(final URL url,
      final DelegationTokenAuthenticatedURL.Token authToken) throws Exception;

  public abstract boolean isKerberosEnabled();

  public abstract String getSpnegoPrincipal();

  public abstract String getSqoopServerKeytabFile();

  /**
   * Get temporary path.
   *
   * @return
   */
  public String getTemporaryPath() {
    return temporaryPath;
  }

  /**
   * Set temporary path.
   *
   * @param temporaryPath
   */
  public void setTemporaryPath(String temporaryPath) {
    this.temporaryPath = temporaryPath;
  }
}
