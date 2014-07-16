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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * Test class for org.apache.sqoop.model.TestMConnector
 */
public class TestMConnector {

//  /**
//   * Test for initialization
//   */
//  @Test
//  public void testInitialization() {
//    List<MForm> forms = new ArrayList<MForm>();
//    MConnectionForms connectionForms1 = new MConnectionForms(forms);
//    MJobForms jobform1 = new MJobForms(MJob.Type.EXPORT, forms);
//    List<MJobForms> jobFormList = new ArrayList<MJobForms>();
//    jobFormList.add(jobform1);
//    MConnector connector1 = new MConnector("NAME", "CLASSNAME", "1.0",
//        connectionForms1, jobFormList);
//    assertEquals("NAME", connector1.getUniqueName());
//    assertEquals("CLASSNAME", connector1.getClassName());
//    assertEquals("1.0", connector1.getVersion());
//    MConnector connector2 = new MConnector("NAME", "CLASSNAME", "1.0",
//        connectionForms1, jobFormList);
//    assertEquals(connector2, connector1);
//    MConnector connector3 = new MConnector("NAME1", "CLASSNAME", "2.0",
//        connectionForms1, jobFormList);
//    assertFalse(connector1.equals(connector3));
//
//    try {
//      connector1 = new MConnector(null, "CLASSNAME", "1.0", connectionForms1,
//          jobFormList); // Expecting null pointer exception
//    } catch (NullPointerException e) {
//      assertTrue(true);
//    }
//    try {
//      connector1 = new MConnector("NAME", null, "1.0", connectionForms1,
//          jobFormList); // Expecting null pointer exception
//    } catch (NullPointerException e) {
//      assertTrue(true);
//    }
//  }
//
//  @Test
//  public void testClone() {
//    List<MForm> forms = new ArrayList<MForm>();
//    MIntegerInput input = new MIntegerInput("INTEGER-INPUT", false);
//    input.setValue(100);
//    MStringInput strInput = new MStringInput("STRING-INPUT",false,(short)20);
//    strInput.setValue("TEST-VALUE");
//    List<MInput<?>> list = new ArrayList<MInput<?>>();
//    list.add(input);
//    list.add(strInput);
//    MForm form = new MForm("FORMNAME", list);
//    forms.add(form);
//    MConnectionForms connectionForms1 = new MConnectionForms(forms);
//    MJobForms jobform1 = new MJobForms(MJob.Type.EXPORT, forms);
//    List<MJobForms> jobFormList = new ArrayList<MJobForms>();
//    jobFormList.add(jobform1);
//    MConnector connector1 = new MConnector("NAME", "CLASSNAME", "1.0",
//        connectionForms1, jobFormList);
//    assertEquals("NAME", connector1.getUniqueName());
//    assertEquals("CLASSNAME", connector1.getClassName());
//    assertEquals("1.0", connector1.getVersion());
//    //Clone with values. Checking values copying after the cloning. But form values will be null
//    MConnector clone1 = connector1.clone(true);
//    assertEquals("NAME", clone1.getUniqueName());
//    assertEquals("CLASSNAME", clone1.getClassName());
//    assertEquals("1.0", clone1.getVersion());
//    MForm clonedForm1 = clone1.getConnectionForms().getForms().get(0);
//    assertNull(clonedForm1.getInputs().get(0).getValue());
//    assertNull(clonedForm1.getInputs().get(1).getValue());
//
//    MForm clonedForm2 = clone1.getJobForms(MJob.Type.EXPORT).getForms().get(0);
//    assertNull(clonedForm2.getInputs().get(0).getValue());
//    assertNull(clonedForm2.getInputs().get(1).getValue());
//
//    //Clone without values. Inputs value will be null after cloning.
//    MConnector clone2 = connector1.clone(false);
//    clonedForm1 = clone2.getConnectionForms().getForms().get(0);
//    assertNull(clonedForm1.getInputs().get(0).getValue());
//    assertNull(clonedForm1.getInputs().get(1).getValue());
//    clonedForm2 = clone2.getJobForms(MJob.Type.EXPORT).getForms().get(0);
//    assertNull(clonedForm2.getInputs().get(0).getValue());
//    assertNull(clonedForm2.getInputs().get(1).getValue());
//  }
}
