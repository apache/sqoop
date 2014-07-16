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
 * Test class for org.apache.sqoop.model.MConnection
 */
public class TestMConnection {

//  /**
//   * Test for initialization
//   */
//  @Test
//  public void testInitialization() {
//    // Test default constructor
//    MConnection connection = connection();
//    assertEquals(123l, connection.getConnectorId());
//    assertEquals("Vampire", connection.getName());
//    assertEquals("Buffy", connection.getCreationUser());
//    assertEquals(forms1(), connection.getConnectorPart());
//    assertEquals(forms2(), connection.getFrameworkPart());
//
//    // Test copy constructor
//    MConnection copy = new MConnection(connection);
//    assertEquals(123l, copy.getConnectorId());
//    assertEquals("Vampire", copy.getName());
//    assertEquals("Buffy", copy.getCreationUser());
//    assertEquals(connection.getCreationDate(), copy.getCreationDate());
//    assertEquals(forms1(), copy.getConnectorPart());
//    assertEquals(forms2(), copy.getFrameworkPart());
//
//    // Test constructor for metadata upgrade (the order of forms is different)
//    MConnection upgradeCopy = new MConnection(connection, forms2(), forms1());
//    assertEquals(123l, upgradeCopy.getConnectorId());
//    assertEquals("Vampire", upgradeCopy.getName());
//    assertEquals("Buffy", upgradeCopy.getCreationUser());
//    assertEquals(connection.getCreationDate(), upgradeCopy.getCreationDate());
//    assertEquals(forms2(), upgradeCopy.getConnectorPart());
//    assertEquals(forms1(), upgradeCopy.getFrameworkPart());
//  }
//
//  @Test
//  public void testClone() {
//    MConnection connection = connection();
//
//    // Clone without value
//    MConnection withoutValue = connection.clone(false);
//    assertEquals(connection, withoutValue);
//    assertEquals(MPersistableEntity.PERSISTANCE_ID_DEFAULT, withoutValue.getPersistenceId());
//    assertNull(withoutValue.getName());
//    assertNull(withoutValue.getCreationUser());
//    assertEquals(forms1(), withoutValue.getConnectorPart());
//    assertEquals(forms2(), withoutValue.getFrameworkPart());
//    assertNull(withoutValue.getConnectorPart().getForm("FORMNAME").getInput("INTEGER-INPUT").getValue());
//    assertNull(withoutValue.getConnectorPart().getForm("FORMNAME").getInput("STRING-INPUT").getValue());
//
//    // Clone with value
//    MConnection withValue = connection.clone(true);
//    assertEquals(connection, withValue);
//    assertEquals(connection.getPersistenceId(), withValue.getPersistenceId());
//    assertEquals(connection.getName(), withValue.getName());
//    assertEquals(connection.getCreationUser(), withValue.getCreationUser());
//    assertEquals(forms1(), withValue.getConnectorPart());
//    assertEquals(forms2(), withValue.getFrameworkPart());
//    assertEquals(100, withValue.getConnectorPart().getForm("FORMNAME").getInput("INTEGER-INPUT").getValue());
//    assertEquals("TEST-VALUE", withValue.getConnectorPart().getForm("FORMNAME").getInput("STRING-INPUT").getValue());
//  }
//
//  private MConnection connection() {
//    MConnection connection = new MConnection(123l, forms1(), forms2());
//    connection.setName("Vampire");
//    connection.setCreationUser("Buffy");
//    return connection;
//  }
//
//  private MConnectionForms forms1() {
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
//    return new MConnectionForms(forms);
//  }
//
//  private MConnectionForms forms2() {
//    List<MForm> forms = new ArrayList<MForm>();
//    MMapInput input = new MMapInput("MAP-INPUT", false);
//    List<MInput<?>> list = new ArrayList<MInput<?>>();
//    list.add(input);
//    MForm form = new MForm("form", list);
//    forms.add(form);
//    return new MConnectionForms(forms);
//  }

}
