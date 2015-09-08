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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.CoreError;

import static org.apache.sqoop.utils.ContextUtils.getUniqueStrings;

/**
 * Configuration manager that loads Sqoop configuration.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@edu.umd.cs.findbugs.annotations.SuppressWarnings("IS2_INCONSISTENT_SYNC")
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
   * The private constructor for the singleton class.
   */
  private SqoopConfiguration() {}

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
    try (InputStream bootstrapPropStream = new FileInputStream(bootstrapConfig)) {
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

    configureClassLoader(ConfigurationConstants.CLASSPATH);
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

  /**
   * Load extra classpath from sqoop configuration.
   * @param classpathProperty
   */
  private synchronized void configureClassLoader(String classpathProperty) {
    LOG.info("Adding jars to current classloader from property: " + classpathProperty);

    // CSV URL list separated by ":".
    String paths = getContext().getString(classpathProperty);

    if (StringUtils.isEmpty(paths)) {
      LOG.debug("Property " + classpathProperty + " is null or empty. Not adding any extra jars.");
      return;
    }

    ClassLoader currentThreadClassLoader = Thread.currentThread().getContextClassLoader();
    if (currentThreadClassLoader == null) {
      throw new SqoopException(CoreError.CORE_0009, "No thread context classloader to override.");
    }

    Set<String> pathSet = getUniqueStrings(paths);
    List<URL> urls = new LinkedList<URL>();

    for (String path : pathSet) {
      try {
        LOG.debug("Found jar in path: " + path);
        URL url = new File(path).toURI().toURL();
        urls.add(url);
        LOG.debug("Using URL: " + url.toString());
      } catch (MalformedURLException e) {
        throw new SqoopException(CoreError.CORE_0009, "Malformed URL found.", e);
      }
    }

    // Chain the current thread classloader so that
    // configured classpath adds to existing classloader.
    // Existing classpath is not changed.
    URLClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]),
        currentThreadClassLoader);
    Thread.currentThread().setContextClassLoader(classLoader);
  }

  private synchronized void configureLogging() {
    Properties props = new Properties();
    for (Map.Entry<String, String> entry : config.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(ConfigurationConstants.PREFIX_LOG_CONFIG)) {
        String logConfigKey = key.substring(
            ConfigurationConstants.PREFIX_GLOBAL_CONFIG.length());
        props.put(logConfigKey, entry.getValue());
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
