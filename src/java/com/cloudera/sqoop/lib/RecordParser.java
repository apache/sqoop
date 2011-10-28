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
package com.cloudera.sqoop.lib;

import org.apache.commons.logging.Log;

/**
 * Parses a record containing one or more fields. Fields are separated
 * by some FIELD_DELIMITER character, e.g. a comma or a ^A character.
 * Records are terminated by a RECORD_DELIMITER character, e.g., a newline.
 *
 * Fields may be (optionally or mandatorily) enclosed by a quoting char
 * e.g., '\"'
 *
 * Fields may contain escaped characters. An escape character may be, e.g.,
 * the '\\' character. Any character following an escape character
 * is treated literally. e.g., '\n' is recorded as an 'n' character, not a
 * newline.
 *
 * Unexpected results may occur if the enclosing character escapes itself.
 * e.g., this cannot parse SQL SELECT statements where the single character
 * ['] escapes to [''].
 *
 * This class is not synchronized. Multiple threads must use separate
 * instances of RecordParser.
 *
 * The fields parsed by RecordParser are backed by an internal buffer
 * which is cleared when the next call to parseRecord() is made. If
 * the buffer is required to be preserved, you must copy it yourself.
 *
 * @deprecated use org.apache.sqoop.lib.RecordParser instead.
 * @see org.apache.sqoop.lib.RecordParser
 */
public final class RecordParser extends org.apache.sqoop.lib.RecordParser {

  public static final Log LOG = org.apache.sqoop.lib.RecordParser.LOG;

  /**
   * An error thrown when parsing fails.
   *
   * @deprecated use org.apache.sqoop.lib.RecordParser.ParseError instead.
   * @see org.apache.sqoop.lib.RecordParser.ParseError
   */
  public static class ParseError
    extends org.apache.sqoop.lib.RecordParser.ParseError {

    public ParseError() {
      super();
    }

    public ParseError(final String msg) {
      super(msg);
    }

    public ParseError(final String msg, final Throwable cause) {
      super(msg, cause);
    }

    public ParseError(final Throwable cause) {
      super(cause);
    }
  }

  public RecordParser(final DelimiterSet delimitersIn) {
    super(delimitersIn);
  }
}
