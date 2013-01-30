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
package org.apache.sqoop.repository;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.utils.ClassUtils;

public class RepositoryManager {

  /**
   * Logger object.
   */
  private static final Logger LOG = Logger.getLogger(RepositoryManager.class);

  /**
   * Private instance to singleton of this class.
   */
  private static RepositoryManager instance;

  /**
   * Create default object by default.
   *
   * Every Sqoop server application needs one so this should not be performance issue.
   */
  static {
    instance = new RepositoryManager();
  }

  /**
   * Return current instance.
   *
   * @return Current instance
   */
  public static RepositoryManager getInstance() {
    return instance;
  }

  /**
   * Allows to set instance in case that it's need.
   *
   * This method should not be normally used as the default instance should be sufficient. One target
   * user use case for this method are unit tests.
   *
   * @param newInstance New instance
   */
  public static void setInstance(RepositoryManager newInstance) {
    instance = newInstance;
  }

  private RepositoryProvider provider;

  public synchronized void initialize() {
    MapContext context = SqoopConfiguration.getInstance().getContext();

    Map<String, String> repoSysProps = context.getNestedProperties(
        RepoConfigurationConstants.SYSCFG_REPO_SYSPROP_PREFIX);

    LOG.info("Setting system properties: " + repoSysProps);

    for (Map.Entry<String, String> entry : repoSysProps.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }

    String repoProviderClassName = context.getString(
        RepoConfigurationConstants.SYSCFG_REPO_PROVIDER);

    if (repoProviderClassName == null
        || repoProviderClassName.trim().length() == 0) {
      throw new SqoopException(RepositoryError.REPO_0001,
          RepoConfigurationConstants.SYSCFG_REPO_PROVIDER);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Repository provider: " + repoProviderClassName);
    }

    Class<?> repoProviderClass =
        ClassUtils.loadClass(repoProviderClassName);

    if (repoProviderClass == null) {
      throw new SqoopException(RepositoryError.REPO_0001,
          repoProviderClassName);
    }

    try {
      provider = (RepositoryProvider) repoProviderClass.newInstance();
    } catch (Exception ex) {
      throw new SqoopException(RepositoryError.REPO_0001,
          repoProviderClassName, ex);
    }

    provider.initialize(context);

    LOG.info("Repository initialized: OK");
  }

  public synchronized void destroy() {
    try {
      provider.destroy();
    } catch (Exception ex) {
      LOG.error("Failed to shutdown repository provider", ex);
    }
  }

  public synchronized Repository getRepository() {
    return provider.getRepository();
  }
}
