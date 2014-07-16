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

//  public void testToForms() {
//    Config config = new Config();
//    config.aForm.a1 = "value";
//
//    List<MForm> formsByInstance = FormUtils.toForms(config);
//    assertEquals(getForms(), formsByInstance);
//    assertEquals("value", formsByInstance.get(0).getInputs().get(0).getValue());
//
//    List<MForm> formsByClass = FormUtils.toForms(Config.class);
//    assertEquals(getForms(), formsByClass);
//
//    List<MForm> formsByBoth = FormUtils.toForms(Config.class, config);
//    assertEquals(getForms(), formsByBoth);
//    assertEquals("value", formsByBoth.get(0).getInputs().get(0).getValue());
//  }
//
//  public void testToFormsMissingAnnotation() {
//    try {
//      FormUtils.toForms(ConfigWithout.class);
//    } catch(SqoopException ex) {
//      assertEquals(ModelError.MODEL_003, ex.getErrorCode());
//      return;
//    }
//
//    fail("Correct exception wasn't thrown");
//  }
//
//  public void testFailureOnPrimitiveType() {
//    PrimitiveConfig config = new PrimitiveConfig();
//
//    try {
//      FormUtils.toForms(config);
//      fail("We were expecting exception for unsupported type.");
//    } catch(SqoopException ex) {
//      assertEquals(ModelError.MODEL_007, ex.getErrorCode());
//    }
//  }
//
//  public void testFillValues() {
//    List<MForm> forms = getForms();
//
//    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("value");
//
//    Config config = new Config();
//
//    FormUtils.fromForms(forms, config);
//    assertEquals("value", config.aForm.a1);
//  }
//
//  public void testFillValuesObjectReuse() {
//    List<MForm> forms = getForms();
//
//    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("value");
//
//    Config config = new Config();
//    config.aForm.a2 = "x";
//    config.bForm.b1 = "y";
//
//    FormUtils.fromForms(forms, config);
//    assertEquals("value", config.aForm.a1);
//    assertNull(config.aForm.a2);
//    assertNull(config.bForm.b2);
//    assertNull(config.bForm.b2);
//  }
//
//  public void testApplyValidation() {
//    Validation validation = getValidation();
//    List<MForm> forms = getForms();
//
//    FormUtils.applyValidation(forms, validation);
//
//    assertEquals(Status.ACCEPTABLE,
//      forms.get(0).getInputs().get(0).getValidationStatus());
//    assertEquals("e1",
//      forms.get(0).getInputs().get(0).getValidationMessage());
//
//    assertEquals(Status.UNACCEPTABLE,
//      forms.get(0).getInputs().get(1).getValidationStatus());
//    assertEquals("e2",
//      forms.get(0).getInputs().get(1).getValidationMessage());
//  }
//
//  public void testJson() {
//    Config config = new Config();
//    config.aForm.a1 = "A";
//    config.bForm.b2 = "B";
//    config.cForm.intValue = 4;
//    config.cForm.map.put("C", "D");
//    config.cForm.enumeration = Enumeration.X;
//
//    String json = FormUtils.toJson(config);
//
//    Config targetConfig = new Config();
//
//    // Old values from should be always removed
//    targetConfig.aForm.a2 = "X";
//    targetConfig.bForm.b1 = "Y";
//    // Nulls in forms shouldn't be an issue either
//    targetConfig.cForm = null;
//
//    FormUtils.fillValues(json, targetConfig);
//
//    assertEquals("A", targetConfig.aForm.a1);
//    assertNull(targetConfig.aForm.a2);
//
//    assertNull(targetConfig.bForm.b1);
//    assertEquals("B", targetConfig.bForm.b2);
//
//    assertEquals((Integer)4, targetConfig.cForm.intValue);
//    assertEquals(1, targetConfig.cForm.map.size());
//    assertTrue(targetConfig.cForm.map.containsKey("C"));
//    assertEquals("D", targetConfig.cForm.map.get("C"));
//    assertEquals(Enumeration.X, targetConfig.cForm.enumeration);
//  }
//
//  protected Validation getValidation() {
//    Map<Validation.FormInput, Validation.Message> messages
//      = new HashMap<Validation.FormInput, Validation.Message>();
//
//    messages.put(
//      new Validation.FormInput("aForm", "a1"),
//      new Validation.Message(Status.ACCEPTABLE, "e1"));
//    messages.put(
//      new Validation.FormInput("aForm", "a2"),
//      new Validation.Message(Status.UNACCEPTABLE, "e2"));
//
//    return new Validation(Status.UNACCEPTABLE, messages);
//  }
//
//  /**
//   * Form structure that corresponds to Config class declared below
//   * @return Form structure
//   */
//  protected List<MForm> getForms() {
//    List<MForm> ret = new LinkedList<MForm>();
//
//    List<MInput<?>> inputs;
//
//    // Form A
//    inputs = new LinkedList<MInput<?>>();
//    inputs.add(new MStringInput("aForm.a1", false, (short)30));
//    inputs.add(new MStringInput("aForm.a2", true, (short)-1));
//    ret.add(new MForm("aForm", inputs));
//
//    // Form B
//    inputs = new LinkedList<MInput<?>>();
//    inputs.add(new MStringInput("bForm.b1", false, (short)2));
//    inputs.add(new MStringInput("bForm.b2", false, (short)3));
//    ret.add(new MForm("bForm", inputs));
//
//    // Form C
//    inputs = new LinkedList<MInput<?>>();
//    inputs.add(new MIntegerInput("cForm.intValue", false));
//    inputs.add(new MMapInput("cForm.map", false));
//    inputs.add(new MEnumInput("cForm.enumeration", false, new String[]{"X", "Y"}));
//    ret.add(new MForm("cForm", inputs));
//
//    return ret;
//  }
//
//  @ConfigurationClass
//  public static class Config {
//
//    public Config() {
//      aForm = new AForm();
//      bForm = new BForm();
//      cForm = new CForm();
//    }
//
//    @Form AForm aForm;
//    @Form BForm bForm;
//    @Form CForm cForm;
//  }
//
//  @ConfigurationClass
//  public static class PrimitiveConfig {
//    @Form DForm dForm;
//  }
//
//  @FormClass
//  public static class AForm {
//    @Input(size = 30)  String a1;
//    @Input(sensitive = true)  String a2;
//  }
//
//  @FormClass
//  public static class BForm {
//    @Input(size = 2) String b1;
//    @Input(size = 3) String b2;
//  }
//
//  @FormClass
//  public static class CForm {
//    @Input Integer intValue;
//    @Input Map<String, String> map;
//    @Input Enumeration enumeration;
//
//    public CForm() {
//      map = new HashMap<String, String>();
//    }
//  }
//
//  @FormClass
//  public static class DForm {
//    @Input int value;
//  }
//
//  public static class ConfigWithout {
//  }
//
//  enum Enumeration {
//    X,
//    Y,
//  }
}
