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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.Direction;
import org.testng.annotations.Test;

public class TestMJob {
  /**
   * Test class for initialization
   */
  @Test
  public void testInitialization() {
    // Test default constructor
    MJob job = job();
    assertEquals(123l, job.getFromConnectorId());
    assertEquals(456l, job.getToConnectorId());
    assertEquals("Buffy", job.getCreationUser());
    assertEquals("Vampire", job.getName());
    assertEquals(fromConfig(), job.getFromJobConfig());
    assertEquals(toConfig(), job.getToJobConfig());
    assertEquals(driverConfig(), job.getDriverConfig());

    // Test copy constructor
    MJob copy = new MJob(job);
    assertEquals(123l, copy.getFromConnectorId());
    assertEquals(456l, copy.getToConnectorId());
    assertEquals("Buffy", copy.getCreationUser());
    assertEquals("Vampire", copy.getName());
    assertEquals(fromConfig(), copy.getFromJobConfig());
    assertEquals(toConfig(), copy.getToJobConfig());
    assertEquals(driverConfig(), copy.getDriverConfig());

    // Test constructor for metadata upgrade (the order of configs is different)
    MJob upgradeCopy = new MJob(job, fromConfig(), toConfig(), driverConfig());
    assertEquals(123l, upgradeCopy.getFromConnectorId());
    assertEquals(456l, upgradeCopy.getToConnectorId());
    assertEquals("Buffy", upgradeCopy.getCreationUser());
    assertEquals("Vampire", upgradeCopy.getName());
    assertEquals(fromConfig(), upgradeCopy.getFromJobConfig());
    assertEquals(toConfig(), upgradeCopy.getToJobConfig());
    assertEquals(driverConfig(), upgradeCopy.getDriverConfig());
  }

  @Test
  public void testClone() {
    MJob job = job();

    // Clone without value
    MJob withoutJobValue = job.clone(false);
    assertEquals(job, withoutJobValue);
    assertEquals(MPersistableEntity.PERSISTANCE_ID_DEFAULT, withoutJobValue.getPersistenceId());
    assertNull(withoutJobValue.getName());
    assertNull(withoutJobValue.getCreationUser());
    assertEquals(fromConfig(), withoutJobValue.getFromJobConfig());
    assertEquals(toConfig(), withoutJobValue.getToJobConfig());
    assertEquals(driverConfig(), withoutJobValue.getDriverConfig());
    assertNull(withoutJobValue.getFromJobConfig()
        .getConfig("CONFIGFROMNAME").getInput("INTEGER-INPUT").getValue());
    assertNull(withoutJobValue.getFromJobConfig()
        .getConfig("CONFIGFROMNAME").getInput("STRING-INPUT").getValue());

    // Clone with value
    MJob withJobValue = job.clone(true);
    assertEquals(job, withJobValue);
    assertEquals(job.getPersistenceId(), withJobValue.getPersistenceId());
    assertEquals(job.getName(), withJobValue.getName());
    assertEquals(job.getCreationUser(), withJobValue.getCreationUser());
    assertEquals(fromConfig(), withJobValue.getFromJobConfig());
    assertEquals(toConfig(), withJobValue.getToJobConfig());
    assertEquals(driverConfig(), withJobValue.getDriverConfig());
    assertEquals(100, withJobValue.getFromJobConfig()
        .getConfig("CONFIGFROMNAME").getInput("INTEGER-INPUT").getValue());
    assertEquals("TEST-VALUE", withJobValue.getFromJobConfig()
        .getConfig("CONFIGFROMNAME").getInput("STRING-INPUT").getValue());  }

  private MJob job() {
    MJob job = new MJob(123l, 456l, 1L, 2L, fromConfig(), toConfig(), driverConfig());
    job.setName("Vampire");
    job.setCreationUser("Buffy");
    return job;
  }

  private MFromConfig fromConfig() {
    List<MConfig> configs = new ArrayList<MConfig>();
    MIntegerInput input = new MIntegerInput("INTEGER-INPUT", false, InputEditable.ANY, StringUtils.EMPTY);
    input.setValue(100);
    MLongInput lInput = new MLongInput("LONG-INPUT", false, InputEditable.ANY, StringUtils.EMPTY);
    lInput.setValue(100L);
    MStringInput strInput = new MStringInput("STRING-INPUT",false, InputEditable.ANY, StringUtils.EMPTY, (short)20);
    strInput.setValue("TEST-VALUE");
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(input);
    list.add(strInput);
    MConfig config = new MConfig("CONFIGFROMNAME", list);
    configs.add(config);
    return new MFromConfig(configs);
  }

  private MToConfig toConfig() {
    List<MConfig> configs = new ArrayList<MConfig>();
    MMapInput input = new MMapInput("MAP-INPUT", false, InputEditable.ANY, StringUtils.EMPTY);
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(input);
    MConfig config = new MConfig("CONFIGTONAME", list);
    configs.add(config);
    return new MToConfig(configs);
  }

  private MDriverConfig driverConfig() {
    List<MConfig> configs = new ArrayList<MConfig>();
    MMapInput input = new MMapInput("MAP-INPUT", false, InputEditable.ANY, StringUtils.EMPTY);
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(input);
    MConfig config = new MConfig("CONFIGDRIVERNAME", list);
    configs.add(config);
    return new MDriverConfig(configs);
  }
}
