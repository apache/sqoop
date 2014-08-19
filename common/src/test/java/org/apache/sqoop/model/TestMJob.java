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

import org.apache.sqoop.common.Direction;
import org.junit.Test;

/**
 * Test class for org.apache.sqoop.model.MJob
 */
public class TestMJob {
  /**
   * Test class for initialization
   */
  @Test
  public void testInitialization() {
    // Test default constructor
    MJob job = job();
    assertEquals(123l, job.getConnectorId(Direction.FROM));
    assertEquals(456l, job.getConnectorId(Direction.TO));
    assertEquals("Buffy", job.getCreationUser());
    assertEquals("Vampire", job.getName());
    assertEquals(fromForms(), job.getConnectorPart(Direction.FROM));
    assertEquals(toForms(), job.getConnectorPart(Direction.TO));
    assertEquals(frameworkForms(), job.getFrameworkPart());

    // Test copy constructor
    MJob copy = new MJob(job);
    assertEquals(123l, copy.getConnectorId(Direction.FROM));
    assertEquals(456l, copy.getConnectorId(Direction.TO));
    assertEquals("Buffy", copy.getCreationUser());
    assertEquals("Vampire", copy.getName());
    assertEquals(fromForms(), copy.getConnectorPart(Direction.FROM));
    assertEquals(toForms(), copy.getConnectorPart(Direction.TO));
    assertEquals(frameworkForms(), copy.getFrameworkPart());

    // Test constructor for metadata upgrade (the order of forms is different)
    MJob upgradeCopy = new MJob(job, fromForms(), toForms(), frameworkForms());
    assertEquals(123l, upgradeCopy.getConnectorId(Direction.FROM));
    assertEquals(456l, upgradeCopy.getConnectorId(Direction.TO));
    assertEquals("Buffy", upgradeCopy.getCreationUser());
    assertEquals("Vampire", upgradeCopy.getName());
    assertEquals(fromForms(), upgradeCopy.getConnectorPart(Direction.FROM));
    assertEquals(toForms(), upgradeCopy.getConnectorPart(Direction.TO));
    assertEquals(frameworkForms(), upgradeCopy.getFrameworkPart());
  }

  @Test
  public void testClone() {
    MJob job = job();

    // Clone without value
    MJob withoutValue = job.clone(false);
    assertEquals(job, withoutValue);
    assertEquals(MPersistableEntity.PERSISTANCE_ID_DEFAULT, withoutValue.getPersistenceId());
    assertNull(withoutValue.getName());
    assertNull(withoutValue.getCreationUser());
    assertEquals(fromForms(), withoutValue.getConnectorPart(Direction.FROM));
    assertEquals(toForms(), withoutValue.getConnectorPart(Direction.TO));
    assertEquals(frameworkForms(), withoutValue.getFrameworkPart());
    assertNull(withoutValue.getConnectorPart(Direction.FROM)
        .getForm("FORMNAME").getInput("INTEGER-INPUT").getValue());
    assertNull(withoutValue.getConnectorPart(Direction.FROM)
        .getForm("FORMNAME").getInput("STRING-INPUT").getValue());

    // Clone with value
    MJob withValue = job.clone(true);
    assertEquals(job, withValue);
    assertEquals(job.getPersistenceId(), withValue.getPersistenceId());
    assertEquals(job.getName(), withValue.getName());
    assertEquals(job.getCreationUser(), withValue.getCreationUser());
    assertEquals(fromForms(), withValue.getConnectorPart(Direction.FROM));
    assertEquals(toForms(), withValue.getConnectorPart(Direction.TO));
    assertEquals(frameworkForms(), withValue.getFrameworkPart());
    assertEquals(100, withValue.getConnectorPart(Direction.FROM)
        .getForm("FORMNAME").getInput("INTEGER-INPUT").getValue());
    assertEquals("TEST-VALUE", withValue.getConnectorPart(Direction.FROM)
        .getForm("FORMNAME").getInput("STRING-INPUT").getValue());  }

  private MJob job() {
    MJob job = new MJob(123l, 456l, 1L, 2L, fromForms(), toForms(), frameworkForms());
    job.setName("Vampire");
    job.setCreationUser("Buffy");
    return job;
  }

  private MJobForms fromForms() {
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
    return new MJobForms(forms);
  }

  private MJobForms toForms() {
    List<MForm> forms = new ArrayList<MForm>();
    MMapInput input = new MMapInput("MAP-INPUT", false);
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(input);
    MForm form = new MForm("form", list);
    forms.add(form);
    return new MJobForms(forms);
  }

  private MJobForms frameworkForms() {
    List<MForm> forms = new ArrayList<MForm>();
    MMapInput input = new MMapInput("MAP-INPUT", false);
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(input);
    MForm form = new MForm("form", list);
    forms.add(form);
    return new MJobForms(forms);
  }
}
