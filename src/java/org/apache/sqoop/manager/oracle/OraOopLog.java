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

/**
 * Class to wrap commons logging to support caching entries.
 */
public class OraOopLog implements org.apache.commons.logging.Log,
    OraOopLogFactory.OraOopLog2 {

  private org.apache.commons.logging.Log log;
  private StringBuilder cache;

  public OraOopLog(org.apache.commons.logging.Log otherLog) {

    this.log = otherLog;
  }

  @Override
  public void debug(Object message) {

    if (cacheLogEntry(message)) {
      return;
    }

    log.debug(message);

  }

  @Override
  public void debug(Object message, Throwable t) {

    if (cacheLogEntry(message)) {
      return;
    }

    log.debug(message, t);
  }

  @Override
  public void error(Object message) {

    if (cacheLogEntry(message)) {
      return;
    }

    log.error(message);
  }

  @Override
  public void error(Object message, Throwable t) {

    if (cacheLogEntry(message)) {
      return;
    }

    log.error(message, t);
  }

  @Override
  public void fatal(Object message) {

    if (cacheLogEntry(message)) {
      return;
    }

    log.fatal(message);
  }

  @Override
  public void fatal(Object message, Throwable t) {

    if (cacheLogEntry(message)) {
      return;
    }

    log.fatal(message, t);
  }

  @Override
  public void info(Object message) {

    if (cacheLogEntry(message)) {
      return;
    }

    log.info(message);
  }

  @Override
  public void info(Object message, Throwable t) {

    if (cacheLogEntry(message)) {
      return;
    }

    log.info(message, t);
  }

  @Override
  public boolean isDebugEnabled() {

    return log.isDebugEnabled();
  }

  @Override
  public boolean isErrorEnabled() {

    return log.isErrorEnabled();
  }

  @Override
  public boolean isFatalEnabled() {

    return log.isFatalEnabled();
  }

  @Override
  public boolean isInfoEnabled() {

    return log.isInfoEnabled();
  }

  @Override
  public boolean isTraceEnabled() {

    return log.isTraceEnabled();
  }

  @Override
  public boolean isWarnEnabled() {

    return log.isWarnEnabled();
  }

  @Override
  public void trace(Object message) {

    log.trace(message);
    cacheLogEntry(message);
  }

  @Override
  public void trace(Object message, Throwable t) {

    if (cacheLogEntry(message)) {
      return;
    }

    log.trace(message, t);
  }

  @Override
  public void warn(Object message) {

    if (cacheLogEntry(message)) {
      return;
    }

    log.warn(message);
  }

  @Override
  public void warn(Object message, Throwable t) {

    if (cacheLogEntry(message)) {
      return;
    }

    log.warn(message, t);
  }

  @Override
  public boolean getCacheLogEntries() {

    return (this.cache != null);
  }

  @Override
  public String getLogEntries() {

    if (this.cache != null) {
      return this.cache.toString();
    } else {
      return "";
    }
  }

  @Override
  public void setCacheLogEntries(boolean value) {

    if (getCacheLogEntries() && !value) {
      this.cache = null;
    } else if (!getCacheLogEntries() && value) {
      this.cache = new StringBuilder();
    }
  }

  @Override
  public void clearCache() {

    if (getCacheLogEntries()) {
      this.cache = new StringBuilder();
    }
  }

  private boolean cacheLogEntry(Object message) {

    boolean result = getCacheLogEntries();

    if (result && message != null) {
      this.cache.append(message.toString());
    }

    return result;
  }

}

