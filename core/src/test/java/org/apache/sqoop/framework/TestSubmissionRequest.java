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
package org.apache.sqoop.framework;

import org.apache.sqoop.utils.ClassUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestSubmissionRequest {

  private SubmissionRequest submissionRequest;

  @Before
  public void initializeSubmissionRequest() {
    submissionRequest = new SubmissionRequest();
  }

  @Test
  public void testAddJar() {
    submissionRequest.addJar("A");
    submissionRequest.addJar("B");
    submissionRequest.addJar("A");

    assertEquals(2, submissionRequest.getJars().size());
    assertEquals("A", submissionRequest.getJars().get(0));
    assertEquals("B", submissionRequest.getJars().get(1));
  }

  @Test
  public void testAddJarForClass() {
    submissionRequest.addJarForClass(TestSubmissionRequest.class);
    submissionRequest.addJarForClass(TestFrameworkValidator.class);

    assertEquals(1, submissionRequest.getJars().size());
    assertTrue(submissionRequest.getJars().contains(ClassUtils.jarForClass(TestSubmissionRequest.class)));
  }

  @Test
  public void testAddJars() {
    submissionRequest.addJars(Arrays.asList("A", "B"));
    submissionRequest.addJars(Arrays.asList("B", "C"));

    assertEquals(3, submissionRequest.getJars().size());
    assertEquals("A", submissionRequest.getJars().get(0));
    assertEquals("B", submissionRequest.getJars().get(1));
    assertEquals("C", submissionRequest.getJars().get(2));
  }
}
