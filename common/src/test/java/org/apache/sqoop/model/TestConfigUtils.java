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

import org.testng.annotations.Test;
import org.testng.Assert;
import org.testng.AssertJUnit;
import static org.testng.AssertJUnit.assertNull;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.SqoopException;

/**
 * Test config utils
 */
public class TestConfigUtils {

  @Test
  public void testConfigs() {
    TestConfiguration config = new TestConfiguration();
    config.aConfig.a1 = "value";

    List<MConfig> configsByInstance = ConfigUtils.toConfigs(config);
    AssertJUnit.assertEquals(getConfigs(), configsByInstance);
    AssertJUnit.assertEquals("value", configsByInstance.get(0).getInputs().get(0).getValue());

    List<MConfig> configsByClass = ConfigUtils.toConfigs(TestConfiguration.class);
    AssertJUnit.assertEquals(getConfigs(), configsByClass);

    List<MConfig> configsByBoth = ConfigUtils.toConfigs(TestConfiguration.class, config);
    AssertJUnit.assertEquals(getConfigs(), configsByBoth);
    AssertJUnit.assertEquals("value", configsByBoth.get(0).getInputs().get(0).getValue());
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testBadConfigInputsWithNonExisitingOverride() {
    TestBadConfiguration config = new TestBadConfiguration();
    config.aBadConfig.a1 = "value";
    ConfigUtils.toConfigs(config);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testBadConfigInputsWithBadOverride() {
    TestBadConfiguration1 config = new TestBadConfiguration1();
    config.aBadConfig1.a1 = "value";
    ConfigUtils.toConfigs(config);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testBadConfigInputsWithSelfOverride() {
    TestBadConfiguration2 config = new TestBadConfiguration2();
    config.aBadConfig2.a1 = "value";
    ConfigUtils.toConfigs(config);
  }

  @Test
  public void testConfigsMissingAnnotation() {
    try {
      ConfigUtils.toConfigs(ConfigWithoutAnnotation.class);
    } catch (SqoopException ex) {
      AssertJUnit.assertEquals(ModelError.MODEL_003, ex.getErrorCode());
      return;
    }

    Assert.fail("Correct exception wasn't thrown");
  }

  @Test
  public void testNonUniqueConfigNameAttributes() {
    try {
      ConfigUtils.toConfigs(ConfigurationWithNonUniqueConfigNameAttribute.class);
    } catch (SqoopException ex) {
      AssertJUnit.assertEquals(ModelError.MODEL_012, ex.getErrorCode());
      return;
    }

    Assert.fail("Correct exception wasn't thrown");
  }

  @Test
  public void testInvalidConfigNameAttribute() {
    try {
      ConfigUtils.toConfigs(ConfigurationWithInvalidConfigNameAttribute.class);
    } catch (SqoopException ex) {
      AssertJUnit.assertEquals(ModelError.MODEL_013, ex.getErrorCode());
      return;
    }
    Assert.fail("Correct exception wasn't thrown");
  }

  @Test
  public void testInvalidConfigNameAttributeLength() {
    try {
      ConfigUtils.toConfigs(ConfigurationWithInvalidConfigNameAttributeLength.class);
    } catch (SqoopException ex) {
      AssertJUnit.assertEquals(ModelError.MODEL_014, ex.getErrorCode());
      return;
    }
    Assert.fail("Correct exception wasn't thrown");
  }

  @Test
  public void testFailureOnPrimitiveType() {
    PrimitiveConfig config = new PrimitiveConfig();

    try {
      ConfigUtils.toConfigs(config);
      Assert.fail("We were expecting exception for unsupported type.");
    } catch (SqoopException ex) {
      AssertJUnit.assertEquals(ModelError.MODEL_007, ex.getErrorCode());
    }
  }

  @Test
  public void testFillValues() {
    List<MConfig> configs = getConfigs();

    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("value");

    TestConfiguration config = new TestConfiguration();

    ConfigUtils.fromConfigs(configs, config);
    AssertJUnit.assertEquals("value", config.aConfig.a1);
  }

  @Test
  public void testFromConfigWithClass() {
    List<MConfig> configs = getConfigs();

    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("value");

    TestConfiguration config = (TestConfiguration) ConfigUtils.fromConfigs(configs,
        TestConfiguration.class);
    AssertJUnit.assertEquals("value", config.aConfig.a1);
  }

  @Test
  public void testFillValuesObjectReuse() {
    List<MConfig> configs = getConfigs();

    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("value");

    TestConfiguration config = new TestConfiguration();
    config.aConfig.a2 = "x";
    config.bConfig.b1 = "y";

    ConfigUtils.fromConfigs(configs, config);
    AssertJUnit.assertEquals("value", config.aConfig.a1);
    assertNull(config.aConfig.a2);
    assertNull(config.bConfig.b2);
    assertNull(config.bConfig.b2);
  }

  @Test
  public void testJson() {
    TestConfiguration config = new TestConfiguration();
    config.aConfig.a1 = "A";
    config.bConfig.b2 = "B";
    config.cConfig.longValue = 4L;
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

    AssertJUnit.assertEquals("A", targetConfig.aConfig.a1);
    assertNull(targetConfig.aConfig.a2);

    assertNull(targetConfig.bConfig.b1);
    AssertJUnit.assertEquals("B", targetConfig.bConfig.b2);

    AssertJUnit.assertEquals((Long) 4L, targetConfig.cConfig.longValue);
    AssertJUnit.assertEquals(1, targetConfig.cConfig.map.size());
    AssertJUnit.assertTrue(targetConfig.cConfig.map.containsKey("C"));
    AssertJUnit.assertEquals("D", targetConfig.cConfig.map.get("C"));
    AssertJUnit.assertEquals(Enumeration.X, targetConfig.cConfig.enumeration);
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
    inputs.add(new MStringInput("aConfig.a1", false, InputEditable.ANY, StringUtils.EMPTY,
        (short) 30));
    inputs.add(new MStringInput("aConfig.a2", true, InputEditable.ANY, StringUtils.EMPTY,
        (short) -1));
    ret.add(new MConfig("aConfig", inputs));

    // Config B
    inputs = new LinkedList<MInput<?>>();
    inputs.add(new MStringInput("bConfig.b1", false, InputEditable.ANY, StringUtils.EMPTY,
        (short) 2));
    inputs.add(new MStringInput("bConfig.b2", false, InputEditable.ANY, StringUtils.EMPTY,
        (short) 3));
    ret.add(new MConfig("bConfig", inputs));

    // Config C
    inputs = new LinkedList<MInput<?>>();
    inputs.add(new MLongInput("cConfig.longValue", false, InputEditable.ANY, StringUtils.EMPTY));
    inputs.add(new MMapInput("cConfig.map", false, InputEditable.ANY, StringUtils.EMPTY));
    inputs.add(new MEnumInput("cConfig.enumeration", false, InputEditable.ANY, StringUtils.EMPTY,
        new String[] { "X", "Y" }));
    ret.add(new MConfig("cConfig", inputs));

    return ret;
  }

  protected List<MConfig> getBadConfigWithSelfOverrideInputs() {
    List<MConfig> ret = new LinkedList<MConfig>();

    List<MInput<?>> inputs;
    // Config A
    inputs = new LinkedList<MInput<?>>();
    inputs.add(new MStringInput("aConfig.a1", false, InputEditable.ANY, "aConfig.a1", (short) 30));
    inputs.add(new MStringInput("aConfig.a2", true, InputEditable.ANY, StringUtils.EMPTY,
        (short) -1));
    ret.add(new MConfig("aConfig", inputs));
    return ret;
  }

  protected List<MConfig> getBadConfigWithNonExistingOverrideInputs() {
    List<MConfig> ret = new LinkedList<MConfig>();

    List<MInput<?>> inputs;
    // Config A
    inputs = new LinkedList<MInput<?>>();
    inputs.add(new MStringInput("aConfig.a1", false, InputEditable.ANY, "aConfig.a3", (short) 30));
    inputs.add(new MStringInput("aConfig.a2", true, InputEditable.ANY, StringUtils.EMPTY,
        (short) -1));
    ret.add(new MConfig("aConfig", inputs));
    return ret;
  }

  protected List<MConfig> getBadConfigWithUserEditableOverrideInputs() {
    List<MConfig> ret = new LinkedList<MConfig>();

    List<MInput<?>> inputs;
    // Config A
    inputs = new LinkedList<MInput<?>>();
    inputs.add(new MStringInput("aConfig.a1", false, InputEditable.ANY, "aConfig.a2", (short) 30));
    inputs.add(new MStringInput("aConfig.a2", true, InputEditable.USER_ONLY, StringUtils.EMPTY,
        (short) -1));
    ret.add(new MConfig("aConfig", inputs));
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
  public static class TestBadConfiguration {

    public TestBadConfiguration() {
      aBadConfig = new ABadConfig();
    }

    @Config
    ABadConfig aBadConfig;
  }

  @ConfigurationClass
  public static class TestBadConfiguration1 {

    public TestBadConfiguration1() {
      aBadConfig1 = new ABadConfig1();
    }

    @Config
    ABadConfig1 aBadConfig1;
  }

  @ConfigurationClass
  public static class TestBadConfiguration2 {

    public TestBadConfiguration2() {
      aBadConfig2 = new ABadConfig2();
    }

    @Config
    ABadConfig2 aBadConfig2;
  }

  @ConfigurationClass
  public static class TestConfiguration {

    public TestConfiguration() {
      aConfig = new AConfig();
      bConfig = new BConfig();
      cConfig = new CConfig();
    }

    @Config
    AConfig aConfig;
    @Config
    BConfig bConfig;
    @Config
    CConfig cConfig;
  }

  @ConfigurationClass
  public static class PrimitiveConfig {
    @Config
    DConfig dConfig;
  }

  @ConfigClass
  public static class AConfig {
    @Input(size = 30)
    String a1;
    @Input(sensitive = true)
    String a2;
  }

  @ConfigClass
  public static class ABadConfig {
    @Input(size = 30, editable = InputEditable.USER_ONLY, overrides = "a5")
    String a1;
    @Input(sensitive = true)
    String a2;
  }

  @ConfigClass
  public static class ABadConfig1 {
    @Input(size = 30, editable = InputEditable.USER_ONLY, overrides = "a2")
    String a1;
    @Input(sensitive = true, editable = InputEditable.USER_ONLY, overrides = "a1")
    String a2;
  }

  @ConfigClass
  public static class ABadConfig2 {
    @Input(size = 30, editable = InputEditable.USER_ONLY, overrides = "a1")
    String a1;
    @Input(sensitive = true, editable = InputEditable.USER_ONLY, overrides = "a2")
    String a2;
  }

  @ConfigClass
  public static class BConfig {
    @Input(size = 2)
    String b1;
    @Input(size = 3)
    String b2;
  }

  @ConfigClass
  public static class CConfig {
    @Input
    Long longValue;
    @Input
    Map<String, String> map;
    @Input
    Enumeration enumeration;

    public CConfig() {
      map = new HashMap<String, String>();
    }
  }

  @ConfigClass
  public static class InvalidConfig {

  }

  @ConfigClass
  public static class DConfig {
    @Input
    int value;
  }

  public static class ConfigWithoutAnnotation {
  }

  enum Enumeration {
    X, Y,
  }
}
