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

package com.cloudera.sqoop.lib;

import junit.framework.TestCase;

/**
 * Test the boolean parser.
 */
public class TestBooleanParser extends TestCase {
  public void testBoolParser() {
    assertTrue(BooleanParser.valueOf("true"));
    assertTrue(BooleanParser.valueOf("TRUE"));
    assertTrue(BooleanParser.valueOf("True"));
    assertTrue(BooleanParser.valueOf("t"));
    assertTrue(BooleanParser.valueOf("T"));
    assertTrue(BooleanParser.valueOf("on"));
    assertTrue(BooleanParser.valueOf("On"));
    assertTrue(BooleanParser.valueOf("ON"));
    assertTrue(BooleanParser.valueOf("yes"));
    assertTrue(BooleanParser.valueOf("yEs"));
    assertTrue(BooleanParser.valueOf("YES"));
    assertTrue(BooleanParser.valueOf("1"));

    assertFalse(BooleanParser.valueOf(null));

    assertFalse(BooleanParser.valueOf("no"));
    assertFalse(BooleanParser.valueOf("false"));
    assertFalse(BooleanParser.valueOf("FALSE"));
    assertFalse(BooleanParser.valueOf("0"));
    assertFalse(BooleanParser.valueOf("off"));
    assertFalse(BooleanParser.valueOf("OFF"));
    assertFalse(BooleanParser.valueOf("anything else in the world"));
  }
}
