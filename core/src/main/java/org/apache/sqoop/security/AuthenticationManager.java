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
package org.apache.sqoop.security;

import org.apache.log4j.Logger;
import org.apache.sqoop.core.Reconfigurable;
import org.apache.sqoop.core.SqoopConfiguration;

/***
 * AuthenticationManager is responsible for manager AuthenticationHandler.
 */
public class AuthenticationManager implements Reconfigurable {

  private static final Logger LOG = Logger.getLogger(AuthenticationManager.class);

  /**
   * Default authentication handler
   */
  public static final String DEFAULT_AUTHENTICATION_HANDLER = "org.apache.sqoop.security.authentication.SimpleAuthenticationHandler";


  /**
   * Default authentication auto upgrade option value
   */
  protected static boolean DEFAULT_AUTO_UPGRADE = false;

  /**
   * Private instance to singleton of this class.
   */
  private static AuthenticationManager instance;

  /**
   * Create default object
   */
  static {
    instance = new AuthenticationManager();
  }

  /**
   * Return current instance.
   *
   * @return Current instance
   */
  public static AuthenticationManager getInstance() {
    return instance;
  }

  /**
   * Allows to set instance in case that it's need.
   * <p/>
   * This method should not be normally used as the default instance should be sufficient. One target
   * user use case for this method are unit tests.
   *
   * @param newInstance New instance
   */
  public static void setInstance(AuthenticationManager newInstance) {
    instance = newInstance;
  }

  /**
   * Private AuthenticiationHandler to singleton of this class.
   */
  private static AuthenticationHandler authenticationHandler;

  /**
   * Return current authentication handler.
   *
   * @return Current authentication handler
   */
  public static AuthenticationHandler getAuthenticationHandler() {
    return authenticationHandler;
  }

  public synchronized void initialize() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Begin authentication manager initialization");
    }

    String handler = SqoopConfiguration.getInstance().getContext().getString(
        SecurityConstants.AUTHENTICATION_HANDLER,
        DEFAULT_AUTHENTICATION_HANDLER).trim();
    authenticationHandler = SecurityFactory.getAuthenticationHandler(handler);
    authenticationHandler.doInitialize();
    authenticationHandler.secureLogin();

    if (LOG.isInfoEnabled()) {
      LOG.info("Authentication loaded.");
    }
  }

  public synchronized void destroy() {
    LOG.trace("Begin authentication manager destroy");
  }

  @Override
  public synchronized void configurationChanged() {
    LOG.info("Begin authentication manager reconfiguring");
    // If there are configuration options for AuthenticationManager,
    // implement the reconfiguration procedure right here.
    LOG.info("Authentication manager reconfigured");
  }

}
