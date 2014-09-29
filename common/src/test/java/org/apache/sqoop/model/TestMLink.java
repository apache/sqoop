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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class for org.apache.sqoop.model.MConnection
 */
public class TestMLink {

  /**
   * Test for initialization
   */
  @Test
  public void testInitialization() {
    // Test default constructor
    MLink link = link();
    assertEquals(123l, link.getConnectorId());
    assertEquals("Vampire", link.getName());
    assertEquals("Buffy", link.getCreationUser());
    assertEquals(forms1(), link.getConnectorPart());
    assertEquals(forms2(), link.getFrameworkPart());

    // Test copy constructor
    MLink copy = new MLink(link);
    assertEquals(123l, copy.getConnectorId());
    assertEquals("Vampire", copy.getName());
    assertEquals("Buffy", copy.getCreationUser());
    assertEquals(link.getCreationDate(), copy.getCreationDate());
    assertEquals(forms1(), copy.getConnectorPart());
    assertEquals(forms2(), copy.getFrameworkPart());

    // Test constructor for metadata upgrade (the order of forms is different)
    MLink upgradeCopy = new MLink(link, forms2(), forms1());
    assertEquals(123l, upgradeCopy.getConnectorId());
    assertEquals("Vampire", upgradeCopy.getName());
    assertEquals("Buffy", upgradeCopy.getCreationUser());
    assertEquals(link.getCreationDate(), upgradeCopy.getCreationDate());
    assertEquals(forms2(), upgradeCopy.getConnectorPart());
    assertEquals(forms1(), upgradeCopy.getFrameworkPart());
  }

  @Test
  public void testClone() {
    MLink link = link();

    // Clone without value
    MLink withoutValue = link.clone(false);
    assertEquals(link, withoutValue);
    assertEquals(MPersistableEntity.PERSISTANCE_ID_DEFAULT, withoutValue.getPersistenceId());
    assertNull(withoutValue.getName());
    assertNull(withoutValue.getCreationUser());
    assertEquals(forms1(), withoutValue.getConnectorPart());
    assertEquals(forms2(), withoutValue.getFrameworkPart());
    assertNull(withoutValue.getConnectorPart().getForm("FORMNAME").getInput("INTEGER-INPUT").getValue());
    assertNull(withoutValue.getConnectorPart().getForm("FORMNAME").getInput("STRING-INPUT").getValue());

    // Clone with value
    MLink withValue = link.clone(true);
    assertEquals(link, withValue);
    assertEquals(link.getPersistenceId(), withValue.getPersistenceId());
    assertEquals(link.getName(), withValue.getName());
    assertEquals(link.getCreationUser(), withValue.getCreationUser());
    assertEquals(forms1(), withValue.getConnectorPart());
    assertEquals(forms2(), withValue.getFrameworkPart());
    assertEquals(100, withValue.getConnectorPart().getForm("FORMNAME").getInput("INTEGER-INPUT").getValue());
    assertEquals("TEST-VALUE", withValue.getConnectorPart().getForm("FORMNAME").getInput("STRING-INPUT").getValue());
  }

  private MLink link() {
    MLink link = new MLink(123l, forms1(), forms2());
    link.setName("Vampire");
    link.setCreationUser("Buffy");
    return link;
  }

  private MConnectionForms forms1() {
    List<MForm> forms = new ArrayList<MForm>();
    MIntegerInput input = new MIntegerInput("INTEGER-INPUT", false);
    input.setValue(100);
    MStringInput strInput = new MStringInput("STRING-INPUT",false,(short)20);
    strInput.setValue("TEST-VALUE");
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(input);
    list.add(strInput);
    MForm form = new MForm("FORMNAME", list);
    forms.add(form);
    return new MConnectionForms(forms);
  }

  private MConnectionForms forms2() {
    List<MForm> forms = new ArrayList<MForm>();
    MMapInput input = new MMapInput("MAP-INPUT", false);
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(input);
    MForm form = new MForm("form", list);
    forms.add(form);
    return new MConnectionForms(forms);
  }

}
