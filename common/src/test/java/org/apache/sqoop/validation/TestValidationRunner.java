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
import org.apache.sqoop.model.Form;
import org.apache.sqoop.model.FormClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.validation.validators.NotEmpty;
import org.apache.sqoop.validation.validators.NotNull;
import org.apache.sqoop.validation.validators.Validator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 */
public class TestValidationRunner {

  @FormClass(validators = {FormA.FormValidator.class})
  public static class FormA {
    @Input(validators = {NotNull.class})
    String notNull;

    public static class FormValidator extends Validator<FormA> {
      @Override
      public void validate(FormA form) {
        if(form.notNull == null) {
          addMessage(Status.UNACCEPTABLE, "null");
        }
        if("error".equals(form.notNull)) {
          addMessage(Status.UNACCEPTABLE, "error");
        }
      }
    }
  }

  @Test
  public void testValidateForm() {
    FormA form = new FormA();
    ValidationRunner runner = new ValidationRunner();
    ValidationResult result;

    // Null string should fail on Input level and should not call form level validators
    form.notNull = null;
    result = runner.validateForm("formName", form);
    assertEquals(Status.UNACCEPTABLE, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey("formName.notNull"));

    // String "error" should trigger form level error, but not Input level
    form.notNull = "error";
    result = runner.validateForm("formName", form);
    assertEquals(Status.UNACCEPTABLE, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey("formName"));

    // Acceptable state
    form.notNull = "This is truly random string";
    result = runner.validateForm("formName", form);
    assertEquals(Status.FINE, result.getStatus());
    assertEquals(0, result.getMessages().size());
  }

  @FormClass
  public static class FormB {
    @Input(validators = {NotNull.class, NotEmpty.class})
    String str;
  }

  @Test
  public void testMultipleValidatorsOnSingleInput() {
    FormB form = new FormB();
    ValidationRunner runner = new ValidationRunner();
    ValidationResult result;

    form.str = null;
    result = runner.validateForm("formName", form);
    assertEquals(Status.UNACCEPTABLE, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey("formName.str"));
    assertEquals(2, result.getMessages().get("formName.str").size());
  }

  @ConfigurationClass(validators = {ConfigurationA.ClassValidator.class})
  public static class ConfigurationA {
    @Form FormA formA;
    public ConfigurationA() {
      formA = new FormA();
    }

    public static class ClassValidator extends Validator<ConfigurationA> {
      @Override
      public void validate(ConfigurationA conf) {
        if("error".equals(conf.formA.notNull)) {
          addMessage(Status.UNACCEPTABLE, "error");
        }
        if("conf-error".equals(conf.formA.notNull)) {
          addMessage(Status.UNACCEPTABLE, "conf-error");
        }
      }
    }
  }

  @Test
  public void testValidate() {
    ConfigurationA conf = new ConfigurationA();
    ValidationRunner runner = new ValidationRunner();
    ValidationResult result;

    // Null string should fail on Input level and should not call form nor class level validators
    conf.formA.notNull = null;
    result = runner.validate(conf);
    assertEquals(Status.UNACCEPTABLE, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey("formA.notNull"));

    // String "error" should trigger form level error, but not Input nor class level
    conf.formA.notNull = "error";
    result = runner.validate(conf);
    assertEquals(Status.UNACCEPTABLE, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey("formA"));

    // String "conf-error" should trigger class level error, but not Input nor Form level
    conf.formA.notNull = "conf-error";
    result = runner.validate(conf);
    assertEquals(Status.UNACCEPTABLE, result.getStatus());
    assertEquals(1, result.getMessages().size());
    assertTrue(result.getMessages().containsKey(""));

    // Valid string
    conf.formA.notNull = "Valid string";
    result = runner.validate(conf);
    assertEquals(Status.FINE, result.getStatus());
    assertEquals(0, result.getMessages().size());
  }
}
