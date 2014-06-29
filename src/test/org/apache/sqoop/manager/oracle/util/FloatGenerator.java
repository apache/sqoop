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

package org.apache.sqoop.manager.oracle.util;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Generates test data for Oracle FLOAT columns.
 */
public class FloatGenerator extends OraOopTestDataGenerator<BigDecimal> {
  private static final int MIN_SCALE = -125;
  private static final int MAX_SCALE = 125;
  private final int precision;

  /**
   * Create a float generator with the specified binary precision.
   *
   * @param precision
   *          The number of bits in the value of generated numbers
   */
  public FloatGenerator(int precision) {
    super();
    this.precision = precision;
  }

  @Override
  public BigDecimal next() {
    BigInteger unscaled = new BigInteger(precision, rng);
    BigDecimal unscaledBD = new BigDecimal(unscaled);
    int scale =
        rng.nextInt(MAX_SCALE - MIN_SCALE + 1) + MIN_SCALE
            - unscaledBD.precision();
    BigDecimal result = new BigDecimal(unscaled, -scale);
    if (rng.nextBoolean()) {
      result = result.negate();
    }
    return result;
  }

}
