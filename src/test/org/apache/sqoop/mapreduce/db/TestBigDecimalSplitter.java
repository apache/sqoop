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

package org.apache.sqoop.mapreduce.db;

import java.math.BigDecimal;

import junit.framework.TestCase;

import com.cloudera.sqoop.mapreduce.db.BigDecimalSplitter;

public class TestBigDecimalSplitter extends TestCase {

  /* Test if the decimal split sizes are generated as expected */
  public void testDecimalTryDivide() {
    BigDecimal numerator = new BigDecimal("2");
    BigDecimal denominator = new BigDecimal("4");
	BigDecimal expected = new BigDecimal("0.5");
	BigDecimalSplitter splitter = new BigDecimalSplitter();
	BigDecimal out = splitter.tryDivide(numerator, denominator);
	assertEquals(expected, out);
  }

  /* Test if the integer split sizes are generated as expected */
  public void testIntegerTryDivide() {
	BigDecimal numerator = new BigDecimal("99");
	BigDecimal denominator = new BigDecimal("3");
	BigDecimal expected = new BigDecimal("33");
	BigDecimalSplitter splitter = new BigDecimalSplitter();
	BigDecimal out = splitter.tryDivide(numerator, denominator);
	assertEquals(expected, out);
  }

  /* Test if the recurring decimal split sizes are generated as expected */
  public void testRecurringTryDivide() {
	BigDecimal numerator = new BigDecimal("1");
	BigDecimal denominator = new BigDecimal("3");
	BigDecimal expected = new BigDecimal("1");
	BigDecimalSplitter splitter = new BigDecimalSplitter();
	BigDecimal out = splitter.tryDivide(numerator, denominator);
	assertEquals(expected, out);
  }

}
