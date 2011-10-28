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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;

import com.cloudera.sqoop.io.LobFile;

/**
 * BlobRef is a wrapper that holds a BLOB either directly, or a
 * reference to a file that holds the BLOB data.
 */
public class BlobRef extends
  com.cloudera.sqoop.lib.LobRef<byte[], BytesWritable, InputStream> {

  public static final Log LOG = LogFactory.getLog(BlobRef.class.getName());

  public BlobRef() {
    super();
  }

  public BlobRef(byte [] bytes) {
    super(new BytesWritable(bytes));
  }

  /**
   * Initialize a BlobRef to an external BLOB.
   * @param file the filename to the BLOB. May be relative to the job dir.
   * @param offset the offset (in bytes) into the LobFile for this record.
   * @param length the length of the record in bytes.
   */
  public BlobRef(String file, long offset, long length) {
    super(file, offset, length);
  }

  @Override
  protected InputStream getExternalSource(LobFile.Reader reader)
      throws IOException {
    return reader.readBlobRecord();
  }

  @Override
  protected InputStream getInternalSource(BytesWritable data) {
    return new ByteArrayInputStream(data.getBytes(), 0, data.getLength());
  }

  @Override
  protected byte [] getInternalData(BytesWritable data) {
    return Arrays.copyOf(data.getBytes(), data.getLength());
  }

  @Override
  protected BytesWritable deepCopyData(BytesWritable data) {
    return new BytesWritable(Arrays.copyOf(data.getBytes(), data.getLength()));
  }

  @Override
  public void readFieldsInternal(DataInput in) throws IOException {
    // For internally-stored BLOBs, the data is a BytesWritable
    // containing the actual data.

    BytesWritable data = getDataObj();

    if (null == data) {
      data = new BytesWritable();
    }
    data.readFields(in);
    setDataObj(data);
  }

  @Override
  public void writeInternal(DataOutput out) throws IOException {
    getDataObj().write(out);
  }

  /**
   * Create a BlobRef based on parsed data from a line of text.
   * This only operates correctly on external blobs; inline blobs are simply
   * returned as null. You should store BLOB data in SequenceFile format
   * if reparsing is necessary.
   * @param inputString the text-based input data to parse.
   * @return a new BlobRef containing a reference to an external BLOB, or
   * an empty BlobRef if the data to be parsed is actually inline.
   */
  public static com.cloudera.sqoop.lib.BlobRef parse(String inputString) {
    // If inputString is of the form 'externalLob(lf,%s,%d,%d)', then this is
    // an external BLOB stored at the LobFile indicated by '%s' with the next
    // two arguments representing its offset and length in the file.
    // Otherwise, it is an inline BLOB, which we don't support parsing of.

    Matcher m = org.apache.sqoop.lib.LobRef.EXTERNAL_MATCHER.get();
    m.reset(inputString);
    if (m.matches()) {
      // This is a LobFile. Extract the filename, offset and len from the
      // matcher.
      return new com.cloudera.sqoop.lib.BlobRef(m.group(1),
          Long.valueOf(m.group(2)), Long.valueOf(m.group(3)));
    } else {
      // This is inline BLOB string data.
      LOG.warn(
          "Reparsing inline BLOB data is not supported; use SequenceFiles.");
      return new com.cloudera.sqoop.lib.BlobRef();
    }
  }
}
