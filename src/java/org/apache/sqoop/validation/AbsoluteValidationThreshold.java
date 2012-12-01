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

package org.apache.sqoop.validation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A specific implementation of ValidationThreshold that validates based on
 * two values being the same.
 *
 * This is used as the default ValidationThreshold implementation unless
 * overridden in configuration.
 */
public class AbsoluteValidationThreshold implements ValidationThreshold {

  private static final Log LOG =
    LogFactory.getLog(AbsoluteValidationThreshold.class.getName());

  @Override
  public void setThresholdValue(long value) {
  }

  static final ValidationThreshold INSTANCE = new AbsoluteValidationThreshold();

  @Override
  @SuppressWarnings("unchecked")
  public boolean compare(Comparable left, Comparable right) {
    LOG.debug("Absolute Validation threshold comparing "
        + left + " with " + right);

    return (Math.abs(left.compareTo(right)) == 0);
  }
}
