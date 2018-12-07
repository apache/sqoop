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

package org.apache.sqoop.testutil;

import org.junit.rules.ExpectedException;

import java.io.IOException;

public class NumericTypesTestUtils {
  // Constants for the basic test case, that doesn't use extra arguments
  // that are required to avoid errors, i.e. padding and default precision and scale.
  public final static boolean SUCCEED_WITHOUT_EXTRA_ARGS = false;
  public final static boolean FAIL_WITHOUT_EXTRA_ARGS = true;
  // Constants for the test case that has padding specified but not default precision and scale.
  public final static boolean SUCCEED_WITH_PADDING_ONLY = false;
  public final static boolean FAIL_WITH_PADDING_ONLY = true;

  /**
   * Adds properties to the given arg builder for decimal precision and scale.
   * @param builder
   */
  public static void addPrecisionAndScale(ArgumentArrayBuilder builder) {
    builder.withProperty("sqoop.avro.logical_types.decimal.default.precision", "38");
    builder.withProperty("sqoop.avro.logical_types.decimal.default.scale", "3");
  }

  /**
   * Enables padding for decimals in avro and parquet import.
   * @param builder
   */
  public static void addPadding(ArgumentArrayBuilder builder) {
    builder.withProperty("sqoop.avro.decimal_padding.enable", "true");
  }

  public static void addEnableAvroDecimal(ArgumentArrayBuilder builder) {
    builder.withProperty("sqoop.avro.logical_types.decimal.enable", "true");
  }

  public static void addEnableParquetDecimal(ArgumentArrayBuilder builder) {
    builder.withProperty("sqoop.parquet.logical_types.decimal.enable", "true");
  }

  public static void configureJunitToExpectFailure(ExpectedException thrown) {
    thrown.expect(IOException.class);
    thrown.expectMessage("Failure during job; return status 1");
  }
}
