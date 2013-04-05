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
 * A specific implementation of ValidationFailureHandler that logs the failure
 * message and the reason with the configured logger. A note of caution in
 * using this since it fails silently by logging the failure to a log file.
 *
 * This is mostly used for testing purposes since this fails silently.
 */
public class LogOnFailureHandler implements ValidationFailureHandler {
  private static final Log LOG =
    LogFactory.getLog(LogOnFailureHandler.class.getName());

  static final ValidationFailureHandler INSTANCE = new LogOnFailureHandler();

  @Override
  public boolean handle(ValidationContext context) throws ValidationException {
    LOG.warn(context.getMessage() + ", Reason: " + context.getReason());
    return true;
  }
}
