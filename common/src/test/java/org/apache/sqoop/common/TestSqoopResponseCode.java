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

/**
 * Test class for org.apache.sqoop.common.SqoopResponseCode
 */
public class TestSqoopResponseCode {

  /**
   * Test for the method getFromCode()
   */
  @Test
  public void testGetFromCode() {
    SqoopResponseCode src = SqoopResponseCode.getFromCode("1000");
    Assert.assertEquals("OK", src.getMessage());
    Assert.assertEquals("1000", src.getCode());

    SqoopResponseCode src1 = SqoopResponseCode.getFromCode("2000");
    Assert.assertEquals("ERROR", src1.getMessage());
    Assert.assertEquals("2000", src1.getCode());
  }
}
