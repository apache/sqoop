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
package org.apache.sqoop.submission.counter;

import org.testng.annotations.Test;

import org.testng.Assert;

/**
 * Test Class for org.apache.sqoop.submission.counter.TestCounters
 */
public class TestCounters {

  /**
   * Test initialization
   */
  @Test
  public void testInitialization() {
    Counters counters = new Counters();
    Assert.assertTrue(counters.isEmpty());
  }

  /**
   * Test add and get CountersGroup
   */
  @Test
  public void testAddGetCounters() {
    Counters counters = new Counters();
    CounterGroup cg = new CounterGroup("sqoop");
    counters.addCounterGroup(cg);
    Assert.assertFalse(counters.isEmpty());
    Assert.assertNotNull(counters.getCounterGroup("sqoop"));
    Assert.assertEquals("sqoop", counters.getCounterGroup("sqoop").getName());
  }

  /**
   * Test for iterator
   */
  @Test
  public void testIterator() {
    Counters counters = new Counters();
    CounterGroup cg1 = new CounterGroup("sqoop1");
    CounterGroup cg2 = new CounterGroup("sqoop2");
    counters.addCounterGroup(cg1);
    counters.addCounterGroup(cg2);
    int count = 0;
    for (CounterGroup cg : counters) {
      count++;
    }
    Assert.assertEquals(2, count);
  }
}
