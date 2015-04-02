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
package org.apache.sqoop.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.CoreError;

/**
 * Configuration manager that loads Sqoop configuration.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class SqoopConfiguration implements Reconfigurable {

  /**
   * Logger object.
   */
  public static final Logger LOG = Logger.getLogger(SqoopConfiguration.class);

  /**
   * Private instance to singleton of this class.
   */
  private static SqoopConfiguration instance;

  /**
   * Create default object by default.
   *
   * Every Sqoop server application needs one so this should not be performance issue.
   */
  static {
    instance = new SqoopConfiguration();
  }

  /**
   * Return current instance.
   *
   * @return Current instance
   */
  public static SqoopConfiguration getInstance() {
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
  public static void setInstance(SqoopConfiguration newInstance) {
    instance = newInstance;
  }

  private File configDir = null;
  private boolean initialized = false;
  private ConfigurationProvider provider = null;
  private Map<String, String> config = null;
  private Map<String, String> oldConfig = null;

  public synchronized void initialize() {
    if (initialized) {
      LOG.warn("Attempt to reinitialize the system, ignoring");
      return;
    }

    String configDirPath = System.getProperty(
        ConfigurationConstants.SYSPROP_CONFIG_DIR);
    if (configDirPath == null || configDirPath.trim().length() == 0) {
      throw new SqoopException(CoreError.CORE_0001, "Environment variable "
          + ConfigurationConstants.SYSPROP_CONFIG_DIR + " is not set.");
    }

    configDir = new File(configDirPath);
    if (!configDir.exists() || !configDir.isDirectory()) {
      throw new SqoopException(CoreError.CORE_0001, configDirPath);
    }

    String bootstrapConfigFilePath = null;
    try {
      String configDirCanonicalPath = configDir.getCanonicalPath();
      bootstrapConfigFilePath = configDirCanonicalPath
              + "/" + ConfigurationConstants.FILENAME_BOOTCFG_FILE;

    } catch (IOException ex) {
      throw new SqoopException(CoreError.CORE_0001, configDirPath, ex);
    }

    File bootstrapConfig = new File(bootstrapConfigFilePath);
    if (!bootstrapConfig.exists() || !bootstrapConfig.isFile()
        || !bootstrapConfig.canRead()) {
      throw new SqoopException(CoreError.CORE_0002, bootstrapConfigFilePath);
    }

    Properties bootstrapProperties = new Properties();
    InputStream bootstrapPropStream = null;
    try {
      bootstrapPropStream = new FileInputStream(bootstrapConfig);
      bootstrapProperties.load(bootstrapPropStream);
    } catch (IOException ex) {
      throw new SqoopException(
          CoreError.CORE_0002, bootstrapConfigFilePath, ex);
    }

    String configProviderClassName = bootstrapProperties.getProperty(
        ConfigurationConstants.BOOTCFG_CONFIG_PROVIDER);

    if (configProviderClassName == null
        || configProviderClassName.trim().length() == 0) {
      throw new SqoopException(
          CoreError.CORE_0003, ConfigurationConstants.BOOTCFG_CONFIG_PROVIDER);
    }

    Class<?> configProviderClass = null;
    try {
      configProviderClass = Class.forName(configProviderClassName);
    } catch (ClassNotFoundException cnfe) {
      LOG.warn("Exception while trying to load configuration provider", cnfe);
    }

    if (configProviderClass == null) {
      ClassLoader ctxLoader = Thread.currentThread().getContextClassLoader();
      if (ctxLoader != null) {
        try {
          configProviderClass = ctxLoader.loadClass(configProviderClassName);
        } catch (ClassNotFoundException cnfe) {
          LOG.warn("Exception while trying to load configuration provider: "
              + configProviderClassName, cnfe);
        }
      }
    }

    if (configProviderClass == null) {
      throw new SqoopException(CoreError.CORE_0004, configProviderClassName);
    }

    try {
      provider = (ConfigurationProvider) configProviderClass.newInstance();
    } catch (Exception ex) {
      throw new SqoopException(CoreError.CORE_0005,
          configProviderClassName, ex);
    }

    // Initialize the configuration provider
    provider.initialize(configDir, bootstrapProperties);
    configurationChanged();

    provider.registerListener(new CoreConfigurationListener(SqoopConfiguration.getInstance()));

    initialized = true;
  }

  public synchronized MapContext getContext() {
    if (!initialized) {
      throw new SqoopException(CoreError.CORE_0007);
    }

    return new MapContext(config);
  }

  public synchronized MapContext getOldContext() {
    if (!initialized) {
      throw new SqoopException(CoreError.CORE_0007);
    }

    if (oldConfig == null) {
      throw new SqoopException(CoreError.CORE_0008);
    }

    return new MapContext(oldConfig);
  }

  public synchronized void destroy() {
    if (provider != null) {
      try {
        provider.destroy();
      } catch (Exception ex) {
        LOG.error("Failed to shutdown configuration provider", ex);
      }
    }
    provider = null;
    configDir = null;
    config = null;
    oldConfig = null;
    initialized = false;
  }

  private synchronized void configureLogging() {
    Properties props = new Properties();
    for (String key : config.keySet()) {
      if (key.startsWith(ConfigurationConstants.PREFIX_LOG_CONFIG)) {
        String logConfigKey = key.substring(
            ConfigurationConstants.PREFIX_GLOBAL_CONFIG.length());
        props.put(logConfigKey, config.get(key));
      }
    }

    PropertyConfigurator.configure(props);
  }

  public ConfigurationProvider getProvider() {
    return provider;
  }

  @Override
  public synchronized void configurationChanged() {
    oldConfig = config;
    config = provider.getConfiguration();
    configureLogging();
  }

  public static class CoreConfigurationListener implements ConfigurationListener {

    private Reconfigurable listener;

    public CoreConfigurationListener(Reconfigurable target) {
      listener = target;
    }

    @Override
    public void configurationChanged() {
      listener.configurationChanged();
    }
  }
}
