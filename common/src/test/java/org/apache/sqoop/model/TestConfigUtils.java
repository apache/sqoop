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
package org.apache.sqoop.model;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.sqoop.common.SqoopException;

/**
 * Test config utils
 */
public class TestConfigUtils extends TestCase {

  public void testConfigs() {
    TestConfiguration config = new TestConfiguration();
    config.aConfig.a1 = "value";

    List<MConfig> configsByInstance = ConfigUtils.toConfigs(config);
    assertEquals(getConfigs(), configsByInstance);
    assertEquals("value", configsByInstance.get(0).getInputs().get(0).getValue());

    List<MConfig> configsByClass = ConfigUtils.toConfigs(TestConfiguration.class);
    assertEquals(getConfigs(), configsByClass);

    List<MConfig> configsByBoth = ConfigUtils.toConfigs(TestConfiguration.class, config);
    assertEquals(getConfigs(), configsByBoth);
    assertEquals("value", configsByBoth.get(0).getInputs().get(0).getValue());
  }

  public void testConfigsMissingAnnotation() {
    try {
      ConfigUtils.toConfigs(ConfigWithoutAnnotation.class);
    } catch(SqoopException ex) {
      assertEquals(ModelError.MODEL_003, ex.getErrorCode());
      return;
    }

    fail("Correct exception wasn't thrown");
  }

  public void testNonUniqueConfigNameAttributes() {
    try {
      ConfigUtils.toConfigs(ConfigurationWithNonUniqueConfigNameAttribute.class);
    } catch (SqoopException ex) {
      assertEquals(ModelError.MODEL_012, ex.getErrorCode());
      return;
    }

    fail("Correct exception wasn't thrown");
  }

  public void testInvalidConfigNameAttribute() {
    try {
      ConfigUtils.toConfigs(ConfigurationWithInvalidConfigNameAttribute.class);
    } catch (SqoopException ex) {
      assertEquals(ModelError.MODEL_013, ex.getErrorCode());
      return;
    }
    fail("Correct exception wasn't thrown");
  }

  public void testInvalidConfigNameAttributeLength() {
    try {
      ConfigUtils.toConfigs(ConfigurationWithInvalidConfigNameAttributeLength.class);
    } catch (SqoopException ex) {
      assertEquals(ModelError.MODEL_014, ex.getErrorCode());
      return;
    }
    fail("Correct exception wasn't thrown");
  }

  public void testFailureOnPrimitiveType() {
    PrimitiveConfig config = new PrimitiveConfig();

    try {
      ConfigUtils.toConfigs(config);
      fail("We were expecting exception for unsupported type.");
    } catch(SqoopException ex) {
      assertEquals(ModelError.MODEL_007, ex.getErrorCode());
    }
  }

  public void testFillValues() {
    List<MConfig> configs = getConfigs();

    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("value");

    TestConfiguration config = new TestConfiguration();

    ConfigUtils.fromConfigs(configs, config);
    assertEquals("value", config.aConfig.a1);
  }

  public void testFromConfigWithClass() {
    List<MConfig> configs = getConfigs();

    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("value");

    TestConfiguration config = (TestConfiguration) ConfigUtils.fromConfigs(configs, TestConfiguration.class);
    assertEquals("value", config.aConfig.a1);
  }

  public void testFillValuesObjectReuse() {
    List<MConfig> configs = getConfigs();

    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("value");

    TestConfiguration config = new TestConfiguration();
    config.aConfig.a2 = "x";
    config.bConfig.b1 = "y";

    ConfigUtils.fromConfigs(configs, config);
    assertEquals("value", config.aConfig.a1);
    assertNull(config.aConfig.a2);
    assertNull(config.bConfig.b2);
    assertNull(config.bConfig.b2);
  }

  public void testJson() {
    TestConfiguration config = new TestConfiguration();
    config.aConfig.a1 = "A";
    config.bConfig.b2 = "B";
    config.cConfig.intValue = 4;
    config.cConfig.map.put("C", "D");
    config.cConfig.enumeration = Enumeration.X;

    String json = ConfigUtils.toJson(config);

    TestConfiguration targetConfig = new TestConfiguration();

    // Old values from should be always removed
    targetConfig.aConfig.a2 = "X";
    targetConfig.bConfig.b1 = "Y";
    // Nulls in configs shouldn't be an issue either
    targetConfig.cConfig = null;

    ConfigUtils.fillValues(json, targetConfig);

    assertEquals("A", targetConfig.aConfig.a1);
    assertNull(targetConfig.aConfig.a2);

    assertNull(targetConfig.bConfig.b1);
    assertEquals("B", targetConfig.bConfig.b2);

    assertEquals((Integer)4, targetConfig.cConfig.intValue);
    assertEquals(1, targetConfig.cConfig.map.size());
    assertTrue(targetConfig.cConfig.map.containsKey("C"));
    assertEquals("D", targetConfig.cConfig.map.get("C"));
    assertEquals(Enumeration.X, targetConfig.cConfig.enumeration);
  }

  /**
   * Config structure that corresponds to Config class declared below
   * @return Config structure
   */
  protected List<MConfig> getConfigs() {
    List<MConfig> ret = new LinkedList<MConfig>();

    List<MInput<?>> inputs;

    // Config A
    inputs = new LinkedList<MInput<?>>();
    inputs.add(new MStringInput("aConfig.a1", false, (short)30));
    inputs.add(new MStringInput("aConfig.a2", true, (short)-1));
    ret.add(new MConfig("aConfig", inputs));

    // Config B
    inputs = new LinkedList<MInput<?>>();
    inputs.add(new MStringInput("bConfig.b1", false, (short)2));
    inputs.add(new MStringInput("bConfig.b2", false, (short)3));
    ret.add(new MConfig("bConfig", inputs));

    // Config C
    inputs = new LinkedList<MInput<?>>();
    inputs.add(new MIntegerInput("cConfig.intValue", false));
    inputs.add(new MMapInput("cConfig.map", false));
    inputs.add(new MEnumInput("cConfig.enumeration", false, new String[]{"X", "Y"}));
    ret.add(new MConfig("cConfig", inputs));

    return ret;
  }

  @ConfigurationClass
  public static class ConfigurationWithNonUniqueConfigNameAttribute {
    public ConfigurationWithNonUniqueConfigNameAttribute() {
      aConfig = new InvalidConfig();
      bConfig = new InvalidConfig();
    }

    @Config(name = "sameName")
    InvalidConfig aConfig;
    @Config(name = "sameName")
    InvalidConfig bConfig;
  }

  @ConfigurationClass
  public static class ConfigurationWithInvalidConfigNameAttribute {
    public ConfigurationWithInvalidConfigNameAttribute() {
      invalidConfig = new InvalidConfig();
    }

    @Config(name = "#_config")
    InvalidConfig invalidConfig;
  }

  @ConfigurationClass
  public static class ConfigurationWithInvalidConfigNameAttributeLength {
    public ConfigurationWithInvalidConfigNameAttributeLength() {
      invalidLengthConfig = new InvalidConfig();
    }

    @Config(name = "longest_config_more_than_30_characers")
    InvalidConfig invalidLengthConfig;
  }

  @ConfigurationClass
  public static class TestConfiguration {

    public TestConfiguration() {
      aConfig = new AConfig();
      bConfig = new BConfig();
      cConfig = new CConfig();
    }

    @Config AConfig aConfig;
    @Config BConfig bConfig;
    @Config CConfig cConfig;
  }

  @ConfigurationClass
  public static class PrimitiveConfig {
    @Config DConfig dConfig;
  }

  @ConfigClass
  public static class AConfig {
    @Input(size = 30)  String a1;
    @Input(sensitive = true)  String a2;
  }

  @ConfigClass
  public static class BConfig {
    @Input(size = 2) String b1;
    @Input(size = 3) String b2;
  }

  @ConfigClass
  public static class CConfig {
    @Input Integer intValue;
    @Input Map<String, String> map;
    @Input Enumeration enumeration;

    public CConfig() {
      map = new HashMap<String, String>();
    }
  }

  @ConfigClass
  public static class InvalidConfig {

  }

  @ConfigClass
  public static class DConfig {
    @Input int value;
  }

  public static class ConfigWithoutAnnotation {
  }

  enum Enumeration {
    X,
    Y,
  }
}