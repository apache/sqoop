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

/**
 * Status modes of a validation process.
 */
public enum Status {
  /**
   * There are no issues, no warnings. Everything is correct.
   */
  FINE,

  /**
   * Validated entity is correct enough to be processed. There might be some
   * warnings, but no errors.
   */
  ACCEPTABLE,

  /**
   * There are serious issues with validated entity. We can't proceed until
   * reported issues will be resolved.
   */
  UNACCEPTABLE,

  ;

  /**
   * Compare multiple statuses and return the worst one.
   *
   * @param statuses Multiple statuses
   * @return The worst status
   */
  public static Status getWorstStatus(Status ... statuses) {
    Status finalStatus = FINE;

    for (Status status : statuses) {
      if (finalStatus.compareTo(status) < 1) {
        finalStatus = status;
      }
    }

    return finalStatus;
  }

  /**
   * Find out if this status object is good enough to proceed.
   *
   * @return True if we can proceed with processing
   */
  public boolean canProceed() {
    return this == FINE || this == ACCEPTABLE;
  }

  /**
   * Return default validation "everything is completely fine".
   *
   * @return Default validation status
   */
  public static Status getDefault() {
    return FINE;
  }
}
