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

import org.testng.Assert;

import org.testng.annotations.Test;

/**
 * Test class for org.apache.sqoop.submission.counter.CounterGroup
 */
public class TestCounterGroup {

  /**
   * CounterGroup initialization
   */
  @Test
  public void testInitialization() {
    CounterGroup cg = new CounterGroup("sqoop");
    Assert.assertEquals("sqoop", cg.getName());
    Assert.assertFalse(cg.iterator().hasNext());

    Counter c1 = new Counter("counter");
    cg.addCounter(c1);
  }

  /**
   * Test for add and get counter
   */
  @Test
  public void testAddGetCounter() {
    CounterGroup cg = new CounterGroup("sqoop");
    Counter c1 = new Counter("counter");
    cg.addCounter(c1);
    Assert.assertNotNull(cg.getCounter("counter"));
    Assert.assertNull(cg.getCounter("NA"));
  }

  /**
   * Test for iterator
   */
  @Test
  public void testIterator() {
    CounterGroup cg = new CounterGroup("sqoop");
    Counter c1 = new Counter("counter1");
    Counter c2 = new Counter("counter2");
    // Adding 2 Counter into CounterGroup
    cg.addCounter(c1);
    cg.addCounter(c2);
    int count = 0;

    for (Counter c : cg) {
      count++;
    }
    Assert.assertEquals(2, count);

    Counter c3 = new Counter("counter3");
    cg.addCounter(c3);
    count = 0;

    for (Counter c : cg) {
      count++;
    }
    Assert.assertEquals(3, count);
  }
}
