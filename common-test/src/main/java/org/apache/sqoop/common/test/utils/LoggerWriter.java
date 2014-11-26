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
package org.apache.sqoop.common.test.utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * PrintWriter implementation that will forward all the messages into given logger.
 */
public class LoggerWriter extends PrintWriter {

  public LoggerWriter(final Logger logger, final Level level) {
    super(new InternalWriter(logger, level));
  }

  private static class InternalWriter extends Writer {

    private final Logger logger;
    private final Level level;

    public InternalWriter(final Logger logger, final Level level) {
      this.logger = logger;
      this.level = level;
    }

    @Override
    public void write(char[] chars, int offset, int len) throws IOException {
      while(len > 0 && (chars[len - 1] == '\n' || chars[len - 1] == '\r')) {
        len--;
      }

      if(len > 0) {
        logger.log(level, String.copyValueOf(chars, offset, len));
      }
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }
  }
}
