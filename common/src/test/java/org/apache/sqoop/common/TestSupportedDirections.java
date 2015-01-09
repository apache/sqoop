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
package org.apache.sqoop.common;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSupportedDirections {

  @Test
  public void testIsDirectionSupported() {
    // Both
    SupportedDirections supportedDirections = new SupportedDirections(true, true);
    Assert.assertTrue(
        supportedDirections.isDirectionSupported(Direction.FROM));
    Assert.assertTrue(
        supportedDirections.isDirectionSupported(Direction.TO));

    // FROM
    supportedDirections = new SupportedDirections(true, false);
    Assert.assertTrue(
        supportedDirections.isDirectionSupported(Direction.FROM));
    Assert.assertFalse(
        supportedDirections.isDirectionSupported(Direction.TO));

    // TO
    supportedDirections = new SupportedDirections(false, true);
    Assert.assertFalse(
        supportedDirections.isDirectionSupported(Direction.FROM));
    Assert.assertTrue(
        supportedDirections.isDirectionSupported(Direction.TO));

    // NONE
    supportedDirections = new SupportedDirections(false, false);
    Assert.assertFalse(
        supportedDirections.isDirectionSupported(Direction.FROM));
    Assert.assertFalse(
        supportedDirections.isDirectionSupported(Direction.TO));
  }

  @Test
  public void testToString() {
    // Both
    SupportedDirections supportedDirections = new SupportedDirections(true, true);
    Assert.assertEquals("FROM/TO", supportedDirections.toString());

    // FROM
    supportedDirections = new SupportedDirections(true, false);
    Assert.assertEquals("FROM", supportedDirections.toString());

    // TO
    supportedDirections = new SupportedDirections(false, true);
    Assert.assertEquals("TO", supportedDirections.toString());

    // NONE
    supportedDirections = new SupportedDirections(false, false);
    Assert.assertEquals("", supportedDirections.toString());
  }
}
