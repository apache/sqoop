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
package org.apache.sqoop.driver;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MStringInput;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 */
public class TestDriverConfigUpgrader {

  DriverUpgrader upgrader;

  @BeforeMethod(alwaysRun = true)
  public void initializeUpgrader() {
    upgrader = new DriverUpgrader();
  }

  /**
   * We take the same configs on input and output and we expect that all values
   * will be correctly transferred.
   */
  @Test
  public void testJobConfigTyeUpgrade() {
    MDriverConfig original = job();
    MDriverConfig target = job();

    original.getStringInput("f1.s1").setValue("A");
    original.getStringInput("f1.s2").setValue("B");
    original.getIntegerInput("f1.i").setValue(3);

    upgrader.upgradeJobConfig(original, target);

    assertEquals("A", target.getStringInput("f1.s1").getValue());
    assertEquals("B", target.getStringInput("f1.s2").getValue());
    assertEquals(3, (long) target.getIntegerInput("f1.i").getValue());
  }

  /**
   * Upgrade scenario when new input has been added to the target forms.
   */
  @Test
  public void testNonExistingInput() {
    MDriverConfig original = job1();
    MDriverConfig target = job2();

    original.getStringInput("f1.s1").setValue("A");
    original.getStringInput("f1.s2").setValue("B");
    original.getIntegerInput("f1.i").setValue(3);

    upgrader.upgradeJobConfig(original, target);

    assertEquals("A", target.getStringInput("f1.s1").getValue());
    assertNull(target.getStringInput("f1.s2_").getValue());
    assertEquals(3, (long) target.getIntegerInput("f1.i").getValue());
  }

  /**
   * Upgrade scenario when entire has been added in the target and therefore is
   * missing in the original.
   */
  @Test
  public void testNonExistingConfig() {
    MDriverConfig original = job1();
    MDriverConfig target = job3();

    original.getStringInput("f1.s1").setValue("A");
    original.getStringInput("f1.s2").setValue("B");
    original.getIntegerInput("f1.i").setValue(3);

    upgrader.upgradeJobConfig(original, target);

    assertNull(target.getStringInput("f2.s1").getValue());
    assertNull(target.getStringInput("f2.s2").getValue());
    assertNull(target.getIntegerInput("f2.i").getValue());
  }

  MDriverConfig job() {
    return new MDriverConfig(configs1());
  }

  MDriverConfig job1() {
    return new MDriverConfig(configs1());
  }

  MDriverConfig job2() {
    return new MDriverConfig(configs2());
  }

  MDriverConfig job3() {
    return new MDriverConfig(configs3());
  }

  List<MConfig> configs1() {
    List<MConfig> list = new LinkedList<MConfig>();
    list.add(new MConfig("f1", inputs1("f1")));
    return list;
  }

  List<MInput<?>> inputs1(String formName) {
    List<MInput<?>> list = new LinkedList<MInput<?>>();
    list.add(new MStringInput(formName + ".s1", false, InputEditable.ANY, StringUtils.EMPTY,
        (short) 30));
    list.add(new MStringInput(formName + ".s2", false, InputEditable.ANY, StringUtils.EMPTY,
        (short) 30));
    list.add(new MIntegerInput(formName + ".i", false, InputEditable.ANY, StringUtils.EMPTY));
    return list;
  }

  List<MConfig> configs2() {
    List<MConfig> list = new LinkedList<MConfig>();
    list.add(new MConfig("f1", inputs2("f1")));
    return list;
  }

  List<MInput<?>> inputs2(String formName) {
    List<MInput<?>> list = new LinkedList<MInput<?>>();
    list.add(new MStringInput(formName + ".s1", false, InputEditable.ANY, StringUtils.EMPTY,
        (short) 30));
    list.add(new MStringInput(formName + ".s2_", false, InputEditable.ANY, StringUtils.EMPTY,
        (short) 30));
    list.add(new MIntegerInput(formName + ".i", false, InputEditable.ANY, StringUtils.EMPTY));
    return list;
  }

  List<MConfig> configs3() {
    List<MConfig> list = new LinkedList<MConfig>();
    list.add(new MConfig("f2", inputs1("f2")));
    return list;
  }
}
