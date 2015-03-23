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

import java.util.Properties;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.core.PropertiesConfigurationProvider;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.core.TestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRepositoryManager {

  @Test
  public void testSystemNotInitialized() throws Exception {
    // Unset any configuration dir if it is set by another test
    System.getProperties().remove(ConfigurationConstants.SYSPROP_CONFIG_DIR);
    Properties bootProps = new Properties();
    bootProps.setProperty(ConfigurationConstants.BOOTCFG_CONFIG_PROVIDER,
        PropertiesConfigurationProvider.class.getCanonicalName());
    Properties configProps = new Properties();
    TestUtils.setupTestConfigurationUsingProperties(bootProps, configProps);
    try {
      SqoopConfiguration.getInstance().initialize();
      RepositoryManager.getInstance().initialize();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof SqoopException);
      Assert.assertSame(((SqoopException) ex).getErrorCode(),
          RepositoryError.REPO_0001);
    }
  }
}
