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
package org.apache.sqoop.lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.regex.Matcher;

import org.apache.hadoop.io.Text;

import com.cloudera.sqoop.io.LobFile;

/**
 * ClobRef is a wrapper that holds a CLOB either directly, or a
 * reference to a file that holds the CLOB data.
 */
public class ClobRef
  extends com.cloudera.sqoop.lib.LobRef<String, String, Reader> {

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

  @Override
  protected Reader getExternalSource(LobFile.Reader reader)
      throws IOException {
    return reader.readClobRecord();
  }

  @Override
  protected Reader getInternalSource(String data) {
    return new StringReader(data);
  }

  @Override
  protected String deepCopyData(String data) {
    return data;
  }

  @Override
  protected String getInternalData(String data) {
    return data;
  }

  @Override
  public void readFieldsInternal(DataInput in) throws IOException {
    // For internally-stored clobs, the data is written as UTF8 Text.
    setDataObj(Text.readString(in));
  }

  @Override
  public void writeInternal(DataOutput out) throws IOException {
    Text.writeString(out, getDataObj());
  }

  /**
   * Create a ClobRef based on parsed data from a line of text.
   * @param inputString the text-based input data to parse.
   * @return a ClobRef to the given data.
   */
  public static com.cloudera.sqoop.lib.ClobRef parse(String inputString) {
    // If inputString is of the form 'externalLob(lf,%s,%d,%d)', then this is
    // an external CLOB stored at the LobFile indicated by '%s' with the next
    // two arguments representing its offset and length in the file.
    // Otherwise, it is an inline CLOB, which we read as-is.

    Matcher m = EXTERNAL_MATCHER.get();
    m.reset(inputString);
    if (m.matches()) {
      // This is a LobFile. Extract the filename, offset and len from the
      // matcher.
      return new com.cloudera.sqoop.lib.ClobRef(m.group(1),
          Long.valueOf(m.group(2)), Long.valueOf(m.group(3)));
    } else {
      // This is inline CLOB string data.
      return new com.cloudera.sqoop.lib.ClobRef(inputString);
    }
  }
}
