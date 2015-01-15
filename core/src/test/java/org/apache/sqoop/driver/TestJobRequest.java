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
package org.apache.sqoop.driver;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.utils.ClassUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJobRequest {

  private JobRequest jobRequest;

  @BeforeMethod(alwaysRun = true)
  public void initializeSubmissionRequest() {
    jobRequest = new JobRequest();
  }

  @Test
  public void testAddJar() {
    jobRequest.addJar("A");
    jobRequest.addJar("B");
    jobRequest.addJar("A");

    assertEquals(2, jobRequest.getJars().size());
    assertTrue(jobRequest.getJars().contains("A"));
    assertTrue(jobRequest.getJars().contains("B"));
  }

  @Test
  public void testAddJarForClass() {
    jobRequest.addJarForClass(TestJobRequest.class);
    assertEquals(1, jobRequest.getJars().size());
    assertTrue(jobRequest.getJars().contains(ClassUtils.jarForClass(TestJobRequest.class)));
  }

  @Test
  public void testAddJars() {
    Set<String> set = new HashSet<String>();
    set.addAll(Arrays.asList("A", "B"));
    jobRequest.addJars(set);
    set = new HashSet<String>();
    set.addAll(Arrays.asList("B", "C"));
    jobRequest.addJars(set);

    assertEquals(3, jobRequest.getJars().size());
    assertTrue(jobRequest.getJars().contains("A"));
    assertTrue(jobRequest.getJars().contains("A"));
    assertTrue(jobRequest.getJars().contains("C"));
  }
}