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

import org.apache.sqoop.common.SqoopException;
import org.junit.Test;

/**
 * Test class for org.apache.sqoop.model.MJob
 */
public class TestMJob {
//  /**
//   * Test class for initialization
//   */
//  @Test
//  public void testInitialization() {
//    // Test default constructor
//    MJob job = job(MJob.Type.IMPORT);
//    assertEquals(123l, job.getFromConnectorId());
//    assertEquals(MJob.Type.IMPORT, job.getType());
//    assertEquals("Buffy", job.getCreationUser());
//    assertEquals("Vampire", job.getName());
//    assertEquals(forms1(MJob.Type.IMPORT), job.getFromPart());
//    assertEquals(forms2(MJob.Type.IMPORT), job.getFrameworkPart());
//
//    // Test copy constructor
//    MJob copy = new MJob(job);
//    assertEquals(123l, copy.getFromConnectorId());
//    assertEquals(MJob.Type.IMPORT, copy.getType());
//    assertEquals("Vampire", copy.getName());
//    assertEquals("Buffy", copy.getCreationUser());
//    assertEquals(job.getCreationDate(), copy.getCreationDate());
//    assertEquals(forms1(MJob.Type.IMPORT), copy.getFromPart());
//    assertEquals(forms2(MJob.Type.IMPORT), copy.getFrameworkPart());
//
//    // Test constructor for metadata upgrade (the order of forms is different)
//    MJob upgradeCopy = new MJob(job, forms2(MJob.Type.IMPORT), forms1(MJob.Type.IMPORT));
//    assertEquals(123l, upgradeCopy.getFromConnectorId());
//    assertEquals(MJob.Type.IMPORT, upgradeCopy.getType());
//    assertEquals("Vampire", upgradeCopy.getName());
//    assertEquals("Buffy", upgradeCopy.getCreationUser());
//    assertEquals(job.getCreationDate(), upgradeCopy.getCreationDate());
//    assertEquals(forms2(MJob.Type.IMPORT), upgradeCopy.getFromPart());
//    assertEquals(forms1(MJob.Type.IMPORT), upgradeCopy.getFrameworkPart());
//  }
//
//  @Test(expected = SqoopException.class)
//  public void testIncorrectDefaultConstructor() {
//    new MJob(1l, 1l, MJob.Type.IMPORT, forms1(MJob.Type.IMPORT), forms2(MJob.Type.EXPORT));
//  }
//
//  @Test(expected = SqoopException.class)
//  public void testIncorrectUpgradeConstructor() {
//    new MJob(job(MJob.Type.EXPORT), forms1(MJob.Type.IMPORT), forms2(MJob.Type.IMPORT));
//  }
//
//  @Test
//  public void testClone() {
//    MJob job = job(MJob.Type.IMPORT);
//
//    // Clone without value
//    MJob withoutValue = job.clone(false);
//    assertEquals(job, withoutValue);
//    assertEquals(MPersistableEntity.PERSISTANCE_ID_DEFAULT, withoutValue.getPersistenceId());
//    assertEquals(MJob.Type.IMPORT, withoutValue.getType());
//    assertNull(withoutValue.getName());
//    assertNull(withoutValue.getCreationUser());
//    assertEquals(forms1(MJob.Type.IMPORT), withoutValue.getFromPart());
//    assertEquals(forms2(MJob.Type.IMPORT), withoutValue.getFrameworkPart());
//    assertNull(withoutValue.getFromPart().getForm("FORMNAME").getInput("INTEGER-INPUT").getValue());
//    assertNull(withoutValue.getFromPart().getForm("FORMNAME").getInput("STRING-INPUT").getValue());
//
//    // Clone with value
//    MJob withValue = job.clone(true);
//    assertEquals(job, withValue);
//    assertEquals(job.getPersistenceId(), withValue.getPersistenceId());
//    assertEquals(MJob.Type.IMPORT, withValue.getType());
//    assertEquals(job.getName(), withValue.getName());
//    assertEquals(job.getCreationUser(), withValue.getCreationUser());
//    assertEquals(forms1(MJob.Type.IMPORT), withValue.getFromPart());
//    assertEquals(forms2(MJob.Type.IMPORT), withValue.getFrameworkPart());
//    assertEquals(100, withValue.getFromPart().getForm("FORMNAME").getInput("INTEGER-INPUT").getValue());
//    assertEquals("TEST-VALUE", withValue.getFromPart().getForm("FORMNAME").getInput("STRING-INPUT").getValue());  }
//
//  private MJob job(MJob.Type type) {
//    MJob job = new MJob(123l, 456l, type, forms1(type), forms2(type));
//    job.setName("Vampire");
//    job.setCreationUser("Buffy");
//    return job;
//  }
//
//  private MJobForms forms1(MJob.Type type) {
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
//    return new MJobForms(type, forms);
//  }
//
//  private MJobForms forms2(MJob.Type type) {
//    List<MForm> forms = new ArrayList<MForm>();
//    MMapInput input = new MMapInput("MAP-INPUT", false);
//    List<MInput<?>> list = new ArrayList<MInput<?>>();
//    list.add(input);
//    MForm form = new MForm("form", list);
//    forms.add(form);
//    return new MJobForms(type, forms);
//  }
}
