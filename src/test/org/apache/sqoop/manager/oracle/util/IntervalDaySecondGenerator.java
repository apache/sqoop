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

/**
 * Generates test data for Oracle INTERVAL DAY TO SECOND columns.
 */
public class IntervalDaySecondGenerator extends
                 OraOopTestDataGenerator<String> {
  private final int daysPrecision;
  private final int minDays;
  private final int maxDays;
  private final int secondsPrecision;
  private final int maxFractionalSeconds;

  /**
   * Create a generator that will generate intervals with the specified
   * precision for days and seconds.
   *
   * @param daysPrecision
   *          Number of decimal digits in the days part of each interval
   * @param secondsPrecision
   *          Number of decimal digits after the decimal point in seconds part
   *          of each interval.
   */
  public IntervalDaySecondGenerator(int daysPrecision, int secondsPrecision) {
    super();
    this.daysPrecision = daysPrecision;
    this.minDays = -(int) Math.pow(10, daysPrecision) + 1;
    this.maxDays = (int) Math.pow(10, daysPrecision) - 1;
    this.secondsPrecision = secondsPrecision;
    this.maxFractionalSeconds = (int) Math.pow(10, secondsPrecision);
  }

  @Override
  public String next() {
    int days = minDays + rng.nextInt(maxDays - minDays + 1);
    int hours = rng.nextInt(24);
    int minutes = rng.nextInt(60);
    int seconds = rng.nextInt(60);
    int fractionalSeconds = rng.nextInt(maxFractionalSeconds);
    String val =
        String.format("%+0" + daysPrecision + "d %02d:%02d:%02d.%0"
            + secondsPrecision + "d", days, hours, minutes, seconds,
            fractionalSeconds);
    return val;
  }
}
