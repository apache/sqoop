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

import java.util.Properties;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.CoreError;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestSqoopConfiguration {

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    // Unset any configuration dir if it is set by another test
    System.getProperties().remove(ConfigurationConstants.SYSPROP_CONFIG_DIR);
    SqoopConfiguration.getInstance().destroy();
  }

  @Test
  public void testConfigurationInitFailure() {
    boolean success = false;
    try {
      SqoopConfiguration.getInstance().initialize();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof SqoopException);
      Assert.assertSame(((SqoopException) ex).getErrorCode(),
          CoreError.CORE_0001);
      success = true;
    }

    Assert.assertTrue(success);
  }

  @Test
  public void testBootstrapConfigurationInitFailure() {
    boolean success = false;
    try {
      String configDirPath = TestUtils.createEmptyConfigDirectory();
      System.setProperty(ConfigurationConstants.SYSPROP_CONFIG_DIR,
          configDirPath);
      SqoopConfiguration.getInstance().initialize();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof SqoopException);
      Assert.assertSame(((SqoopException) ex).getErrorCode(),
          CoreError.CORE_0002);
      success = true;
    }

    Assert.assertTrue(success);
  }

  @Test
  public void testConfigurationProviderNotSet() throws Exception {
    boolean success = false;
    Properties bootProps = new Properties();
    bootProps.setProperty("foo", "bar");
    TestUtils.setupTestConfigurationUsingProperties(bootProps, null);
    try {
      SqoopConfiguration.getInstance().initialize();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof SqoopException);
      Assert.assertSame(((SqoopException) ex).getErrorCode(),
          CoreError.CORE_0003);
      success = true;
    }

    Assert.assertTrue(success);
  }

  @Test
  public void testConfigurationProviderInvalid() throws Exception {
    boolean success = false;
    Properties bootProps = new Properties();
    bootProps.setProperty(ConfigurationConstants.BOOTCFG_CONFIG_PROVIDER,
        "foobar");
    TestUtils.setupTestConfigurationUsingProperties(bootProps, null);
    try {
      SqoopConfiguration.getInstance().initialize();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof SqoopException);
      Assert.assertSame(((SqoopException) ex).getErrorCode(),
          CoreError.CORE_0004);

      success = true;
    }

    Assert.assertTrue(success);
  }

  @Test
  public void testConfiugrationProviderCannotLoad() throws Exception {
    boolean success = false;
    Properties bootProps = new Properties();
    bootProps.setProperty(ConfigurationConstants.BOOTCFG_CONFIG_PROVIDER,
        MockInvalidConfigurationProvider.class.getCanonicalName());
    TestUtils.setupTestConfigurationUsingProperties(bootProps, null);
    try {
      SqoopConfiguration.getInstance().initialize();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof SqoopException);
      Assert.assertSame(((SqoopException) ex).getErrorCode(),
          CoreError.CORE_0005);
      success = true;
    }

    Assert.assertTrue(success);
  }

  @Test
  public void testPropertiesConfigProviderNoFile() throws Exception {
    boolean success = false;
    Properties bootProps = new Properties();
    bootProps.setProperty(ConfigurationConstants.BOOTCFG_CONFIG_PROVIDER,
        PropertiesConfigurationProvider.class.getCanonicalName());
    TestUtils.setupTestConfigurationUsingProperties(bootProps, null);
    try {
      SqoopConfiguration.getInstance().initialize();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof SqoopException);
      Assert.assertSame(((SqoopException) ex).getErrorCode(),
          CoreError.CORE_0006);
      success = true;
    }

    Assert.assertTrue(success);
  }

  @Test
  public void testSystemNotInitialized() throws Exception {
    boolean success = false;
    try {
      SqoopConfiguration.getInstance().getContext();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof SqoopException);
      Assert.assertSame(((SqoopException) ex).getErrorCode(),
          CoreError.CORE_0007);
      success = true;
    }

    Assert.assertTrue(success);
  }

  @Test
  public void testConfigurationInitSuccess() throws Exception {
    TestUtils.setupTestConfigurationWithExtraConfig(null, null);
    SqoopConfiguration.getInstance().initialize();
  }
}
