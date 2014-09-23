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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.utils.ClassUtils;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestJobRequest {

  private JobRequest jobRequest;

  @Before
  public void initializeSubmissionRequest() {
    jobRequest = new JobRequest();
  }

  @Test
  public void testAddJar() {
    jobRequest.addJar("A");
    jobRequest.addJar("B");
    jobRequest.addJar("A");

    assertEquals(2, jobRequest.getJars().size());
    assertEquals("A", jobRequest.getJars().get(0));
    assertEquals("B", jobRequest.getJars().get(1));
  }

  @Test
  public void testAddJarForClass() {
    jobRequest.addJarForClass(TestJobRequest.class);
    assertEquals(1, jobRequest.getJars().size());
    assertTrue(jobRequest.getJars().contains(ClassUtils.jarForClass(TestJobRequest.class)));
  }

  @Test
  public void testAddJars() {
    jobRequest.addJars(Arrays.asList("A", "B"));
    jobRequest.addJars(Arrays.asList("B", "C"));

    assertEquals(3, jobRequest.getJars().size());
    assertEquals("A", jobRequest.getJars().get(0));
    assertEquals("B", jobRequest.getJars().get(1));
    assertEquals("C", jobRequest.getJars().get(2));
  }
}