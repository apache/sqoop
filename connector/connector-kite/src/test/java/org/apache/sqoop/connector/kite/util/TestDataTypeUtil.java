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
package org.apache.sqoop.connector.kite.util;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 */
public class TestDataTypeUtil {
  @Test
  public void testToAvroName() {
    // All valid cases
    assertEquals("name", KiteDataTypeUtil.toAvroName("name"));
    assertEquals("nN9", KiteDataTypeUtil.toAvroName("nN9"));
    assertEquals("_nN9", KiteDataTypeUtil.toAvroName("_nN9"));
    assertEquals("_", KiteDataTypeUtil.toAvroName("_"));

    // Ensuring that first character is valid
    assertEquals("_9", KiteDataTypeUtil.toAvroName("9"));
    assertEquals("__", KiteDataTypeUtil.toAvroName("%"));

    // Rest of the string
    assertEquals("_____________", KiteDataTypeUtil.toAvroName("!@#$%^&*()_+"));
  }
}
