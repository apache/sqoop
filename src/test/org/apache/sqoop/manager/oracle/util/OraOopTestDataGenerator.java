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

import java.util.Random;

/**
 * Abstract framework class for generating test data.
 *
 * @param <T>
 *          The type that will be generated
 */
public abstract class OraOopTestDataGenerator<T> {
  protected Random rng;
  private long seed;

  /**
   * Initialise with a default seed for the random number generator.
   */
  public OraOopTestDataGenerator() {
    this(0);
  }

  /**
   * Initialise with a given seed for the random number generator.
   *
   * @param seed
   *          The seed to initialise the rng with.
   */
  public OraOopTestDataGenerator(long seed) {
    this.seed = seed;
    if (seed == 0) {
      rng = new Random();
    } else {
      rng = new Random(seed);
    }
  }

  /**
   * Reset the rng to its initial state.
   */
  public void reset() {
    rng = new Random(seed);
  }

  /**
   * @return The next item of test data. The same sequence will be re-generated
   *         after a call to reset.
   */
  public abstract T next();
}
