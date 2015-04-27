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
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.core.Reconfigurable;
import org.apache.sqoop.core.SqoopConfiguration;

/**
 * AuthorizationManager is responsible for managing AuthorizationHandler.
 */
public class AuthorizationManager implements Reconfigurable {

  private static final Logger LOG = Logger.getLogger(AuthorizationManager.class);

  /**
   * Default authorization handler
   */
  public static final String DEFAULT_AUTHORIZATION_HANDLER = "org.apache.sqoop.security.authorization.DefaultAuthorizationHandler";

  /**
   * Default authentication provider
   */
  public static final String DEFAULT_AUTHENTICATION_PROVIDER = "org.apache.sqoop.security.authorization.DefaultAuthenticationProvider";

  /**
   * Default authentication provider
   */
  public static final String DEFAULT_SERVER_NAME = "SqoopServer1";

  /**
   * Default authorization auto upgrade option value
   */
  protected static boolean DEFAULT_AUTO_UPGRADE = false;

  /**
   * Private instance to singleton of this class.
   */
  private static AuthorizationManager instance;

  /**
   * Create default object
   */
  static {
    instance = new AuthorizationManager();
  }

  /**
   * Return current instance.
   *
   * @return Current instance
   */
  public static AuthorizationManager getInstance() {
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
  public static void setInstance(AuthorizationManager newInstance) {
    instance = newInstance;
  }

  /**
   * Private AuthenticiationHandler to singleton of this class.
   */
  private static AuthorizationHandler authorizationHandler;

  /**
   * Return current authorization handler.
   *
   * @return Current authorization handler
   */
  public static AuthorizationHandler getAuthorizationHandler() {
    return authorizationHandler;
  }

  public synchronized void initialize() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    LOG.trace("Begin authorization manager initialization");

    String handler = SqoopConfiguration.getInstance().getContext().getString(
            SecurityConstants.AUTHORIZATION_HANDLER,
            DEFAULT_AUTHORIZATION_HANDLER).trim();
    authorizationHandler = SecurityFactory.getAuthorizationHandler(handler);

    String provider = SqoopConfiguration.getInstance().getContext().getString(
            SecurityConstants.AUTHENTICATION_PROVIDER,
            DEFAULT_AUTHENTICATION_PROVIDER).trim();

    String serverName = SqoopConfiguration.getInstance().getContext().getString(
            SecurityConstants.SERVER_NAME,
            DEFAULT_SERVER_NAME).trim();

    authorizationHandler.doInitialize(SecurityFactory.getAuthenticationProvider(provider), serverName);

    LOG.info("Authorization loaded.");
  }

  public synchronized void destroy() {
    LOG.trace("Begin authorization manager destroy");
  }

  @Override
  public synchronized void configurationChanged() {
    LOG.info("Begin authorization manager reconfiguring");
    // If there are configuration options for AuthorizationManager,
    // implement the reconfiguration procedure right here.
    LOG.info("Authorization manager reconfigured");
  }

}
