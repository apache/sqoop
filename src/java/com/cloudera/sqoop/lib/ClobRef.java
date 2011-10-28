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

/**
 * ClobRef is a wrapper that holds a CLOB either directly, or a
 * reference to a file that holds the CLOB data.
 *
 * @deprecated use org.apache.sqoop.lib.ClobRef instead.
 * @see org.apache.sqoop.lib.ClobRef
 */
public class ClobRef extends org.apache.sqoop.lib.ClobRef {

  public ClobRef() {
    super();
  }

  public ClobRef(String chars) {
    super(chars);
  }

  /**
   * Initialize a clobref to an external CLOB.
   * @param file the filename to the CLOB. May be relative to the job dir.
   * @param offset the offset (in bytes) into the LobFile for this record.
   * @param length the length of the record in characters.
   */
  public ClobRef(String file, long offset, long length) {
    super(file, offset, length);
  }

  /**
   * Create a ClobRef based on parsed data from a line of text.
   * @param inputString the text-based input data to parse.
   * @return a ClobRef to the given data.
   */
  public static ClobRef parse(String inputString) {
    return org.apache.sqoop.lib.ClobRef.parse(inputString);
  }
}

