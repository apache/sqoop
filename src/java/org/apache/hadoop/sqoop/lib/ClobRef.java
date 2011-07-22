/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package org.apache.hadoop.sqoop.lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * ClobRef is a wrapper that holds a Clob either directly, or a
 * reference to a file that holds the clob data.
 */
public class ClobRef implements Writable {

  public ClobRef(String chars) {
    this.fileName = null;
    this.data = chars;
  }

  public ClobRef() {
    this.fileName = null;
    this.data = null;
  }

  /**
   * Initialize a clobref to an external CLOB.
   * @param file the filename to the CLOB. May be relative to the job dir.
   * @param ignored is not used; this just differentiates this constructor
   * from ClobRef(String chars).
   */
  public ClobRef(String file, boolean ignored) {
    this.fileName = file;
    this.data = null;
  }

  // If the data is 'small', it's held directly, here.
  private String data;

  // If there data is too large, it's written into a file
  // whose path (relative to the rest of the dataset) is recorded here.
  // This takes precedence if this value is non-null.
  private String fileName;

  /**
   * @return true if the CLOB data is in an external file; false if
   * it is materialized inline.
   */
  public boolean isExternal() {
    return fileName != null;
  }

  /**
   * Convenience method to access #getDataReader(Configuration, Path)
   * from within a map task that read this ClobRef from a file-based
   * InputSplit.
   * @param mapContext the Mapper.Context instance that encapsulates
   * the current map task.
   * @return a Reader to access the CLOB data.
   * @throws IllegalArgumentException if it cannot find the source
   * path for this CLOB based on the MapContext.
   * @throws IOException if it could not read the CLOB from external storage.
   */
  public Reader getDataReader(MapContext mapContext)
      throws IllegalArgumentException, IOException {
    InputSplit split = mapContext.getInputSplit();
    if (split instanceof FileSplit) {
      Path basePath = ((FileSplit) split).getPath().getParent();
      return getDataReader(mapContext.getConfiguration(),
        basePath);
    } else {
      throw new IllegalArgumentException(
          "Could not ascertain CLOB base path from MapContext.");
    }
  }

  /**
   * Get access to the CLOB data itself.
   * This method returns a Reader-based representation of the
   * CLOB data, accessing the filesystem for external CLOB storage
   * as necessary.
   * @param conf the Configuration used to access the filesystem
   * @param basePath the base directory where the table records are
   * stored.
   * @return a Reader used to read the CLOB data.
   * @throws IOException if it could not read the CLOB from external storage.
   */
  public Reader getDataReader(Configuration conf, Path basePath)
      throws IOException {
    if (isExternal()) {
      // use external storage.
      FileSystem fs = FileSystem.get(conf);
      return new InputStreamReader(fs.open(new Path(basePath, fileName)));
    } else {
      return new StringReader(data);
    }
  }

  /**
   * @return a string representation of the ClobRef. If this is an
   * inline clob (isExternal() returns false), it will contain the
   * materialized data. Otherwise it returns a description of the
   * reference. To ensure access to the data itself, {@see #getDataStream()}.
   */
  @Override
  public String toString() {
    if (isExternal()) {
      return "externalClob(" + fileName + ")";
    } else {
      return data;
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // The serialization format for this object is:
    // boolean isExternal
    // if true, the next field is a String containing the external file name.
    // if false, the next field is String containing the actual data.

    boolean isIndirect = in.readBoolean();
    if (isIndirect) {
      this.fileName = Text.readString(in);
      this.data = null;
    } else {
      this.fileName = null;
      this.data = Text.readString(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    boolean isIndirect = isExternal();
    out.writeBoolean(isIndirect);
    if (isIndirect) {
      Text.writeString(out, fileName);
    } else {
      Text.writeString(out, data);
    }
  }

  // A pattern matcher which can recognize external CLOB data
  // vs. an inline CLOB string.
  private static final ThreadLocal<Matcher> EXTERNAL_MATCHER =
    new ThreadLocal<Matcher>() {
      @Override protected Matcher initialValue() {
        Pattern externalPattern = Pattern.compile("externalClob\\((.*)\\)");
        return externalPattern.matcher("");
      }
    };

  /**
   * Create a ClobRef based on parsed data from a line of text.
   * @param inputString the text-based input data to parse.
   * @return a ClobRef to the given data.
   */
  public static ClobRef parse(String inputString) {
    // If inputString is of the form 'externalClob(%s)', then this is an
    // external CLOB stored at the filename indicated by '%s'. Otherwise,
    // it is an inline CLOB.

    Matcher m = EXTERNAL_MATCHER.get();
    m.reset(inputString);
    if (m.matches()) {
      // Extract the filename component from the string.
      return new ClobRef(m.group(1), true);
    } else {
      // This is inline CLOB string data.
      return new ClobRef(inputString);
    }
  }
}

