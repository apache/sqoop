/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.framework;

import org.apache.sqoop.model.*;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 */
public class TestFrameworkMetadataUpgrader {

  FrameworkMetadataUpgrader upgrader;

  @Before
  public void initializeUpgrader() {
    upgrader = new FrameworkMetadataUpgrader();
  }

  /**
   * We take the same forms on input and output and we
   * expect that all values will be correctly transferred.
   */
  @Test
  public void testConnectionUpgrade() {
    MConnectionForms original = connection1();
    MConnectionForms target = connection1();

    original.getStringInput("f1.s1").setValue("A");
    original.getStringInput("f1.s2").setValue("B");
    original.getIntegerInput("f1.i").setValue(3);

    upgrader.upgrade(original, target);

    assertEquals("A", target.getStringInput("f1.s1").getValue());
    assertEquals("B", target.getStringInput("f1.s2").getValue());
    assertEquals(3, (long)target.getIntegerInput("f1.i").getValue());
  }

  /**
   * We take the same forms on input and output and we
   * expect that all values will be correctly transferred.
   */
  @Test
  public void testJobUpgrade() {
    MJobForms original = job1();
    MJobForms target = job1();

    original.getStringInput("f1.s1").setValue("A");
    original.getStringInput("f1.s2").setValue("B");
    original.getIntegerInput("f1.i").setValue(3);

    upgrader.upgrade(original, target);

    assertEquals("A", target.getStringInput("f1.s1").getValue());
    assertEquals("B", target.getStringInput("f1.s2").getValue());
    assertEquals(3, (long)target.getIntegerInput("f1.i").getValue());
  }

  /**
   * Upgrade scenario when new input has been added to the target forms.
   */
  @Test
  public void testNonExistingInput() {
    MConnectionForms original = connection1();
    MConnectionForms target = connection2();

    original.getStringInput("f1.s1").setValue("A");
    original.getStringInput("f1.s2").setValue("B");
    original.getIntegerInput("f1.i").setValue(3);

    upgrader.upgrade(original, target);

    assertEquals("A", target.getStringInput("f1.s1").getValue());
    assertNull(target.getStringInput("f1.s2_").getValue());
    assertEquals(3, (long)target.getIntegerInput("f1.i").getValue());
  }

  /**
   * Upgrade scenario when entire has been added in the target and
   * therefore is missing in the original.
   */
  @Test
  public void testNonExistingForm() {
    MConnectionForms original = connection1();
    MConnectionForms target = connection3();

    original.getStringInput("f1.s1").setValue("A");
    original.getStringInput("f1.s2").setValue("B");
    original.getIntegerInput("f1.i").setValue(3);

    upgrader.upgrade(original, target);

    assertNull(target.getStringInput("f2.s1").getValue());
    assertNull(target.getStringInput("f2.s2").getValue());
    assertNull(target.getIntegerInput("f2.i").getValue());
  }

  MJobForms job1() {
    return new MJobForms(forms1());
  }

  MConnectionForms connection1() {
    return new MConnectionForms(forms1());
  }

  MConnectionForms connection2() {
    return new MConnectionForms(forms2());
  }

  MConnectionForms connection3() {
    return new MConnectionForms(forms3());
  }

  List<MForm> forms1() {
    List<MForm> list = new LinkedList<MForm>();
    list.add(new MForm("f1", inputs1("f1")));
    return list;
  }

  List<MInput<?>> inputs1(String formName) {
    List<MInput<?>> list = new LinkedList<MInput<?>>();
    list.add(new MStringInput(formName + ".s1", false, (short)30));
    list.add(new MStringInput(formName + ".s2", false, (short)30));
    list.add(new MIntegerInput(formName + ".i", false));
    return list;
  }

  List<MForm> forms2() {
    List<MForm> list = new LinkedList<MForm>();
    list.add(new MForm("f1", inputs2("f1")));
    return list;
  }

  List<MInput<?>> inputs2(String formName) {
    List<MInput<?>> list = new LinkedList<MInput<?>>();
    list.add(new MStringInput(formName + ".s1", false, (short)30));
    list.add(new MStringInput(formName + ".s2_", false, (short)30));
    list.add(new MIntegerInput(formName + ".i", false));
    return list;
  }

  List<MForm> forms3() {
    List<MForm> list = new LinkedList<MForm>();
    list.add(new MForm("f2", inputs1("f2")));
    return list;
  }
}
