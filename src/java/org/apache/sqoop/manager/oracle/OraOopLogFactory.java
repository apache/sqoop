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

package org.apache.sqoop.manager.oracle;

import org.apache.commons.logging.LogFactory;

/**
 * Wraps commons logging.
 */
public final class OraOopLogFactory {
  private OraOopLogFactory() {
  }

  /**
   * Interface for log entries including caching for test purposes.
   */
  public interface OraOopLog2 {

    boolean getCacheLogEntries();

    void setCacheLogEntries(boolean value);

    String getLogEntries();

    void clearCache();
  }

  public static OraOopLog getLog(Class<?> clazz) {

    return OraOopLogFactory.getLog(clazz.getName());
  }

  public static OraOopLog getLog(String logName) {

    return new OraOopLog(LogFactory.getLog(logName));
  }

}
