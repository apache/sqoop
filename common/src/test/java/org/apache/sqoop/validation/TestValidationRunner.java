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
package org.apache.sqoop.validation;

import org.apache.sqoop.model.ConfigurationClass;
import org.apache.sqoop.model.Config;
import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.validators.Contains;
import org.apache.sqoop.validation.validators.NotEmpty;
import org.apache.sqoop.validation.validators.NotNull;
import org.apache.sqoop.validation.validators.AbstractValidator;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 */
public class TestValidationRunner {

  @ConfigClass(validators = {@Validator(ConfigA.ConfigValidator.class)})
  public static class ConfigA {
    @Input(validators = {@Validator(NotNull.class)})
    String notNull;

    public static class ConfigValidator extends AbstractValidator<ConfigA> {
      @Override
      public void validate(ConfigA config) {
        if(config.notNull == null) {
          addMessage(Status.ERROR, "null");
        }
        if("error".equals(config.notNull)) {
          addMessage(Status.ERROR, "error");
        }
      }
    }
  }

  @Test
  public void testValidateConfig() {
    ConfigA config = new ConfigA();
    ConfigValidationRunner runner = new ConfigValidationRunner();
    ConfigValidationResult result;

    // Null string should fail on Input level and should not call config level validators
    config.notNull = null;
    result = runner.validateConfig("configName", config);
    assertEquals(Status.ERROR, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey("configName.notNull"));

    // String "error" should trigger config level error, but not Input level
    config.notNull = "error";
    result = runner.validateConfig("configName", config);
    assertEquals(Status.ERROR, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey("configName"));

    // Acceptable state
    config.notNull = "This is truly random string";
    result = runner.validateConfig("configName", config);
    assertEquals(Status.OK, result.getStatus());
    assertEquals(0, result.getMessages().size());
  }

  @ConfigClass
  public static class ConfigB {
    @Input(validators = {@Validator(NotNull.class), @Validator(NotEmpty.class)})
    String str;
  }

  @ConfigClass
  public static class ConfigC {
    @Input(validators = {@Validator(value = Contains.class, strArg = "findme")})
    String str;
  }

  @Test
  public void testMultipleValidatorsOnSingleInput() {
    ConfigB config = new ConfigB();
    ConfigValidationRunner runner = new ConfigValidationRunner();
    ConfigValidationResult result;

    config.str = null;
    result = runner.validateConfig("configName", config);
    assertEquals(Status.ERROR, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey("configName.str"));
    assertEquals(2, result.getMessages().get("configName.str").size());
  }

  @Test
  public void testValidatorWithParameters() {
    ConfigC config = new ConfigC();
    ConfigValidationRunner runner = new ConfigValidationRunner();
    ConfigValidationResult result;

    // Sub string not found
    config.str = "Mordor";
    result = runner.validateConfig("configName", config);
    assertEquals(Status.ERROR, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey("configName.str"));

    // Sub string found
    config.str = "Morfindmedor";
    result = runner.validateConfig("configName", config);
    assertEquals(Status.OK, result.getStatus());
    assertEquals(0, result.getMessages().size());
  }

  @ConfigurationClass(validators = {@Validator(ConfigurationA.ClassValidator.class)})
  public static class ConfigurationA {
    @Config ConfigA formA;
    public ConfigurationA() {
      formA = new ConfigA();
    }

    public static class ClassValidator extends AbstractValidator<ConfigurationA> {
      @Override
      public void validate(ConfigurationA conf) {
        if("error".equals(conf.formA.notNull)) {
          addMessage(Status.ERROR, "error");
        }
        if("conf-error".equals(conf.formA.notNull)) {
          addMessage(Status.ERROR, "conf-error");
        }
      }
    }
  }

  @Test
  public void testValidate() {
    ConfigurationA conf = new ConfigurationA();
    ConfigValidationRunner runner = new ConfigValidationRunner();
    ConfigValidationResult result;

    // Null string should fail on Input level and should not call config nor class level validators
    conf.formA.notNull = null;
    result = runner.validate(conf);
    assertEquals(Status.ERROR, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey("formA.notNull"));

    // String "error" should trigger config level error, but not Input nor class level
    conf.formA.notNull = "error";
    result = runner.validate(conf);
    assertEquals(Status.ERROR, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey("formA"));

    // String "conf-error" should trigger class level error, but not Input nor Config level
    conf.formA.notNull = "conf-error";
    result = runner.validate(conf);
    assertEquals(Status.ERROR, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey(""));

    // Valid string
    conf.formA.notNull = "Valid string";
    result = runner.validate(conf);
    assertEquals(Status.OK, result.getStatus());
    assertEquals(0, result.getMessages().size());
  }
}
