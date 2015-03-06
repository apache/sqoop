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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.CoreError;

import static org.apache.sqoop.core.ConfigurationConstants.PROPERTIES_PROVIDER_SLEEP;

public class PropertiesConfigurationProvider implements ConfigurationProvider {

  private static final Logger LOG =
      Logger.getLogger(PropertiesConfigurationProvider.class);

  public static final String CONFIG_FILENAME = "sqoop.properties";

  public static final long DEFAULT_SLEEP_TIME = 60000;

  private Map<String, String> configuration = new HashMap<String, String>();

  private List<ConfigurationListener> listeners =
      new ArrayList<ConfigurationListener>();

  private File configFile;

  private ConfigFilePoller poller;

  public PropertiesConfigurationProvider() {
    // Default constructor
  }

  @Override
  public synchronized void registerListener(ConfigurationListener listener) {
    listeners.add(listener);
  }

  @Override
  public synchronized Map<String, String> getConfiguration() {
    Map<String, String> config = new HashMap<String, String>();
    config.putAll(configuration);

    return config;
  }

  @Override
  public synchronized void destroy() {
    LOG.info("Shutting down configuration poller thread");
    if (poller != null) {
      poller.setShutdown();
      poller.interrupt();
      try {
        poller.join();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    poller = null;
  }

  @Override
  public synchronized void initialize(
      File configDir, Properties bootstrapConfiguration) {
    configFile = new File(configDir, CONFIG_FILENAME);
    if (!configFile.exists() || !configFile.isFile() || !configFile.canRead()) {
      throw new SqoopException(CoreError.CORE_0006, configFile.getPath());
    }

    loadConfiguration(false); // at least one load must succeed
    poller = new ConfigFilePoller(configFile);
    LOG.info("Starting config file poller thread");
    poller.start();
  }

  private synchronized void loadConfiguration(boolean notifyListeners) {
    Properties configProperties = new Properties();
    InputStream fis = null;
    try {
      fis = new BufferedInputStream(new FileInputStream(configFile));
      configProperties.load(fis);
    } catch (Exception ex) {
      throw new SqoopException(CoreError.CORE_0006, configFile.getPath(), ex);
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException ex) {
          LOG.error("Failed to close file stream for configuration", ex);
        }
      }
    }

    configuration.clear();
    Enumeration<?> propNameEnum = configProperties.propertyNames();
    while (propNameEnum.hasMoreElements()) {
      String name = (String) propNameEnum.nextElement();
      configuration.put(name, configProperties.getProperty(name));
    }

    if (notifyListeners) {
      for (ConfigurationListener configListener : listeners) {
        configListener.configurationChanged();
      }
    }
  }

  private class ConfigFilePoller extends Thread {
    private File file;

    private long lastUpdatedAt;

    private boolean shutdown;

    private long sleepTime;

    ConfigFilePoller(File configFile) {
      this.file = configFile;
      lastUpdatedAt = configFile.lastModified();
      this.setName("sqoop-config-file-poller");
      this.setDaemon(true);
      loadSleepTime();
    }

    synchronized void setShutdown() {
      shutdown = true;
    }

    private synchronized boolean isShutdown() {
      return shutdown;
    }

    private synchronized void loadSleepTime() {
      try {
        String value = configuration.get(PROPERTIES_PROVIDER_SLEEP);
        sleepTime = Long.valueOf(value);
      } catch(Exception e) {
        LOG.debug("Can't load sleeping period from configuration file,"
          + " using default value " + DEFAULT_SLEEP_TIME, e);
        sleepTime = DEFAULT_SLEEP_TIME;
      }
    }

    @Override
    public void run() {

      while(true) {
        LOG.trace("Checking config file for changes: " + file);

        if (file.lastModified() > lastUpdatedAt) {
          LOG.info("Configuration file change detected, attempting to load");
          try {
            lastUpdatedAt = file.lastModified();
            loadConfiguration(true);
            loadSleepTime();
          } catch (Exception ex) {
            LOG.error("Exception while loading configuration", ex);
          }
        }

        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }

        if (isShutdown()) {
          break;
        }
      }
    }
  }
}
