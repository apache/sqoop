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

import junit.framework.TestCase;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Test form utils
 */
public class TestFormUtils extends TestCase {

  public void testToForms() {
    Configuration Configuration = new Configuration();
    Configuration.aForm.a1 = "value";

    List<MForm> formsByInstance = FormUtils.toForms(Configuration);
    assertEquals(getForms(), formsByInstance);
    assertEquals("value", formsByInstance.get(0).getInputs().get(0).getValue());

    List<MForm> formsByClass = FormUtils.toForms(Configuration.class);
    assertEquals(getForms(), formsByClass);

    List<MForm> formsByBoth = FormUtils.toForms(Configuration.class, Configuration);
    assertEquals(getForms(), formsByBoth);
    assertEquals("value", formsByBoth.get(0).getInputs().get(0).getValue());
  }

  public void testToFormsMissingAnnotation() {
    try {
      FormUtils.toForms(ConfigurationWithoutAnnotation.class);
    } catch(SqoopException ex) {
      assertEquals(ModelError.MODEL_003, ex.getErrorCode());
      return;
    }

    fail("Correct exception wasn't thrown");
  }

  public void testNonUniqueFormNameAttributes() {
    try {
      FormUtils.toForms(ConfigurationWithNonUniqueFormNameAttribute.class);
    } catch (SqoopException ex) {
      assertEquals(ModelError.MODEL_012, ex.getErrorCode());
      return;
    }

    fail("Correct exception wasn't thrown");
  }

  public void testInvalidFormNameAttribute() {
    try {
      FormUtils.toForms(ConfigurationWithInvalidFormNameAttribute.class);
    } catch (SqoopException ex) {
      assertEquals(ModelError.MODEL_013, ex.getErrorCode());
      return;
    }

    fail("Correct exception wasn't thrown");
  }

  public void testInvalidFormNameAttributeLength() {
    try {
      FormUtils.toForms(ConfigurationWithInvalidFormNameAttributeLength.class);
    } catch (SqoopException ex) {
      assertEquals(ModelError.MODEL_014, ex.getErrorCode());
      return;
    }
    fail("Correct exception wasn't thrown");
  }

  public void testFailureOnPrimitiveType() {
    PrimitiveConfiguration Configuration = new PrimitiveConfiguration();

    try {
      FormUtils.toForms(Configuration);
      fail("We were expecting exception for unsupported type.");
    } catch(SqoopException ex) {
      assertEquals(ModelError.MODEL_007, ex.getErrorCode());
    }
  }

  public void testFillValues() {
    List<MForm> forms = getForms();
    assertEquals("test_AForm", forms.get(0).getName());
    assertEquals("test_BForm", forms.get(1).getName());
    assertEquals("cForm", forms.get(2).getName());

    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("value");

    Configuration Configuration = new Configuration();

    FormUtils.fromForms(forms, Configuration);
    assertEquals("value", Configuration.aForm.a1);
  }

  public void testFillValuesObjectReuse() {
    List<MForm> forms = getForms();

    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("value");

    Configuration Configuration = new Configuration();
    Configuration.aForm.a2 = "x";
    Configuration.bForm.b1 = "y";

    FormUtils.fromForms(forms, Configuration);
    assertEquals("value", Configuration.aForm.a1);
    assertNull(Configuration.aForm.a2);
    assertNull(Configuration.bForm.b2);
    assertNull(Configuration.bForm.b2);
  }

  public void testApplyValidation() {
    Validation validation = getValidation();
    List<MForm> forms = getForms();

    FormUtils.applyValidation(forms, validation);

    assertEquals(Status.ACCEPTABLE,
      forms.get(0).getInputs().get(0).getValidationStatus());
    assertEquals("e1",
      forms.get(0).getInputs().get(0).getValidationMessage());

    assertEquals(Status.UNACCEPTABLE,
      forms.get(0).getInputs().get(1).getValidationStatus());
    assertEquals("e2",
      forms.get(0).getInputs().get(1).getValidationMessage());
  }

  public void testJson() {
    Configuration Configuration = new Configuration();
    Configuration.aForm.a1 = "A";
    Configuration.bForm.b2 = "B";
    Configuration.cForm.intValue = 4;
    Configuration.cForm.map.put("C", "D");
    Configuration.cForm.enumeration = Enumeration.X;

    String json = FormUtils.toJson(Configuration);

    Configuration targetConfig = new Configuration();

    // Old values from should be always removed
    targetConfig.aForm.a2 = "X";
    targetConfig.bForm.b1 = "Y";
    // Nulls in forms shouldn't be an issue either
    targetConfig.cForm = null;

    FormUtils.fillValues(json, targetConfig);

    assertEquals("A", targetConfig.aForm.a1);
    assertNull(targetConfig.aForm.a2);

    assertNull(targetConfig.bForm.b1);
    assertEquals("B", targetConfig.bForm.b2);

    assertEquals((Integer)4, targetConfig.cForm.intValue);
    assertEquals(1, targetConfig.cForm.map.size());
    assertTrue(targetConfig.cForm.map.containsKey("C"));
    assertEquals("D", targetConfig.cForm.map.get("C"));
    assertEquals(Enumeration.X, targetConfig.cForm.enumeration);
  }

  protected Validation getValidation() {
    Map<Validation.FormInput, Validation.Message> messages
      = new HashMap<Validation.FormInput, Validation.Message>();

    messages.put(
      new Validation.FormInput("test_AForm", "a1"),
      new Validation.Message(Status.ACCEPTABLE, "e1"));
    messages.put(
      new Validation.FormInput("test_AForm", "a2"),
      new Validation.Message(Status.UNACCEPTABLE, "e2"));

    return new Validation(Status.UNACCEPTABLE, messages);
  }

  /**
   * Form structure that corresponds to Configuration class declared below
   * @return Form structure
   */
  protected List<MForm> getForms() {
    List<MForm> ret = new LinkedList<MForm>();

    List<MInput<?>> inputs;

    // Form A
    inputs = new LinkedList<MInput<?>>();
    inputs.add(new MStringInput("test_AForm.a1", false, (short)30));
    inputs.add(new MStringInput("test_AForm.a2", true, (short)-1));
    ret.add(new MForm("test_AForm", inputs));

    // Form B
    inputs = new LinkedList<MInput<?>>();
    inputs.add(new MStringInput("test_BForm.b1", false, (short)2));
    inputs.add(new MStringInput("test_BForm.b2", false, (short)3));
    ret.add(new MForm("test_BForm", inputs));

    // Form C
    inputs = new LinkedList<MInput<?>>();
    inputs.add(new MIntegerInput("cForm.intValue", false));
    inputs.add(new MMapInput("cForm.map", false));
    inputs.add(new MEnumInput("cForm.enumeration", false, new String[]{"X", "Y"}));
    ret.add(new MForm("cForm", inputs));

    return ret;
  }

  @ConfigurationClass
  public static class ConfigurationWithNonUniqueFormNameAttribute {
    public ConfigurationWithNonUniqueFormNameAttribute() {
      aForm = new InvalidForm();
      bForm = new InvalidForm();
    }

    @Form(name = "sameName")
    InvalidForm aForm;
    @Form(name = "sameName")
    InvalidForm bForm;
  }

  @ConfigurationClass
  public static class ConfigurationWithInvalidFormNameAttribute {
    public ConfigurationWithInvalidFormNameAttribute() {
      invalidForm = new InvalidForm();
    }

    @Form(name = "#_form")
    InvalidForm invalidForm;
  }

  @ConfigurationClass
  public static class ConfigurationWithInvalidFormNameAttributeLength {
    public ConfigurationWithInvalidFormNameAttributeLength() {
      invalidLengthForm = new InvalidForm();
    }

    @Form(name = "longest_form_more_than_30_characers")
    InvalidForm invalidLengthForm;
  }

  @ConfigurationClass
  public static class Configuration {

    public Configuration() {
      aForm = new AForm();
      bForm = new BForm();
      cForm = new CForm();
    }

    @Form(name = "test_AForm")
    AForm aForm;
    @Form(name = "test_BForm")
    BForm bForm;
    @Form(name = "")
    CForm cForm;
  }

  @ConfigurationClass
  public static class PrimitiveConfiguration {
    @Form DForm dForm;
  }

  @FormClass
  public static class AForm {
    @Input(size = 30)  String a1;
    @Input(sensitive = true)  String a2;
  }

  @FormClass
  public static class BForm {
    @Input(size = 2) String b1;
    @Input(size = 3) String b2;
  }

  @FormClass
  public static class CForm {
    @Input Integer intValue;
    @Input Map<String, String> map;
    @Input Enumeration enumeration;

    public CForm() {
      map = new HashMap<String, String>();
    }
  }

  @FormClass
  public static class InvalidForm {

  }
  @FormClass
  public static class DForm {
    @Input int value;
  }

  public static class ConfigurationWithoutAnnotation {
  }

  enum Enumeration {
    X,
    Y,
  }
}
