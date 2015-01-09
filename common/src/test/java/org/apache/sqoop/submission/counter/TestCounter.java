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
 * Test class for org.apache.sqoop.submission.counter.Counter
 */
public class TestCounter {

  /**
   * Test method for initialization
   */
  @Test
  public void testInitialization() {
    Counter counter = new Counter("sqoop");
    Assert.assertEquals("sqoop", counter.getName());
    Assert.assertEquals(0l, counter.getValue());

    Counter counter1 = new Counter("sqoop", 1000l);
    Assert.assertEquals("sqoop", counter1.getName());
    Assert.assertEquals(1000l, counter1.getValue());

    counter1.setValue(2000l);
    Assert.assertEquals(2000l, counter1.getValue());
  }
}
