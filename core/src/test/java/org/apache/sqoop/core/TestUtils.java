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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.log4j.Logger;

public class TestUtils {

  private static final Logger LOG = Logger.getLogger(TestUtils.class);

  public static final String NEWLINE =
      System.getProperty("line.separator", "\n");

  public static String createEmptyConfigDirectory() throws Exception {
    File tempDir = null;
    File targetDir = new File("target");
    if (targetDir.exists() && targetDir.isDirectory()) {
      tempDir = targetDir;
    } else {
      tempDir = new File(System.getProperty("java.io.tmpdir"));
    }

    File tempFile = File.createTempFile("test", "config", tempDir);
    String tempConfigDirPath = tempFile.getCanonicalPath() + ".dir/config";
    if (!tempFile.delete()) {
      throw new Exception("Unable to delete tempfile: " + tempFile);
    }

    File tempConfigDir = new File(tempConfigDirPath);
    if (!tempConfigDir.mkdirs()) {
      throw new Exception("Unable to create temp config dir: "
              + tempConfigDirPath);
    }

    return tempConfigDirPath;
  }

  public static void setupTestConfigurationUsingProperties(
      Properties bootstrapProps, Properties props)
    throws Exception {

    String tempConfigDirPath = createEmptyConfigDirectory();
    File tempConfigDir = new File(tempConfigDirPath);

    File bootconfigFile = new File(tempConfigDir,
        ConfigurationConstants.FILENAME_BOOTCFG_FILE);

    if (!bootconfigFile.createNewFile()) {
      throw new Exception("Unable to create config file: " + bootconfigFile);
    }

    if (bootstrapProps != null) {
      BufferedWriter bootconfigWriter = null;
      try {
        bootconfigWriter = new BufferedWriter(new FileWriter(bootconfigFile));

        Enumeration<?> bootstrapPropNames = bootstrapProps.propertyNames();
        while (bootstrapPropNames.hasMoreElements()) {
          String name = (String) bootstrapPropNames.nextElement();
          String value = bootstrapProps.getProperty(name);
          bootconfigWriter.write(name + " = " + value + NEWLINE);
        }

        bootconfigWriter.flush();
      } finally {
        if (bootconfigWriter != null) {
          try {
            bootconfigWriter.close();
          } catch (IOException ex) {
            LOG.error("Failed to close config file writer", ex);
          }
        }
      }
    }

    File sysconfigFile = new File(tempConfigDir,
        PropertiesConfigurationProvider.CONFIG_FILENAME);

    if (props != null) {
      BufferedWriter sysconfigWriter = null;
      try {
        sysconfigWriter = new BufferedWriter(new FileWriter(sysconfigFile));

        Enumeration<?> propNameEnum = props.propertyNames();
        while (propNameEnum.hasMoreElements()) {
          String name = (String) propNameEnum.nextElement();
          String value = props.getProperty(name);
          sysconfigWriter.write(name + " = " + value + NEWLINE);
        }
        sysconfigWriter.flush();
      } finally {
        if (sysconfigWriter != null) {
          try {
            sysconfigWriter.close();
          } catch (IOException ex) {
            LOG.error("Failed to close log config file writer", ex);
          }
        }
      }
    }
    System.setProperty(ConfigurationConstants.SYSPROP_CONFIG_DIR,
        tempConfigDirPath);
  }

  /**
   * Sets up the test configuration using any properties specified in the
   * test file test_config.properties. If the parameter <tt>extraConfig</tt>
   * is specified, it is added to these properties. Consequently any property
   * that exists in both the test_config.properties and the supplied extra
   * properties will retain the value from the later.
   *
   * @param extraConfig any properties that you would like to set in the system
   * @throws Exception
   */
  public static void setupTestConfigurationWithExtraConfig(
      Properties extraBootstrapConfig, Properties extraConfig) throws Exception
  {

    Properties props = new Properties();

    InputStream istream = null;
    try {
      istream = ClassLoader.getSystemResourceAsStream("test_config.properties");
      props.load(istream);
    } finally {
      if (istream != null) {
        try {
          istream.close();
        } catch (Exception ex) {
          LOG.warn("Failed to close input stream", ex);
        }
      }
    }

    if (props.size() == 0) {
      throw new Exception("Unable to load test_config.properties");
    }

    if (extraConfig != null && extraConfig.size() > 0) {
      props.putAll(extraConfig);
    }

    Properties bootstrapProps = new Properties();
    bootstrapProps.setProperty("sqoop.config.provider",
        PropertiesConfigurationProvider.class.getCanonicalName());

    if (extraBootstrapConfig != null && extraBootstrapConfig.size() > 0) {
      bootstrapProps.putAll(extraBootstrapConfig);
    }

    setupTestConfigurationUsingProperties(bootstrapProps, props);
  }


  private TestUtils() {
    // Disable explicit object creation
  }
}
