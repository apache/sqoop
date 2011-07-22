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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * BlobRef is a wrapper that holds a Blob either directly, or a
 * reference to a file that holds the blob data.
 */
public class BlobRef implements Writable {

  public static final Log LOG = LogFactory.getLog(BlobRef.class.getName());

  public BlobRef(byte [] bytes) {
    this.fileName = null;
    this.data = new BytesWritable(bytes);
  }

  public BlobRef() {
    this.fileName = null;
    this.data = null;
  }

  public BlobRef(String file) {
    this.fileName = file;
    this.data = null;
  }

  // If the data is 'small', it's held directly, here.
  private BytesWritable data;

  // If there data is too large, it's written into a file
  // whose path (relative to the rest of the dataset) is recorded here.
  // This takes precedence if this value is non-null.
  private String fileName;

  /**
   * @return true if the BLOB data is in an external file; false if
   * it materialized inline.
   */
  public boolean isExternal() {
    return fileName != null;
  }

  /**
   * Convenience method to access #getDataStream(Configuration, Path)
   * from within a map task that read this BlobRef from a file-based
   * InputSplit.
   * @param mapContext the Mapper.Context instance that encapsulates
   * the current map task.
   * @return an InputStream to access the BLOB data.
   * @throws IllegalArgumentException if it cannot find the source
   * path for this BLOB based on the MapContext.
   * @throws IOException if it could not read the BLOB from external storage.
   */
  public InputStream getDataStream(MapContext mapContext)
      throws IllegalArgumentException, IOException {
    InputSplit split = mapContext.getInputSplit();
    if (split instanceof FileSplit) {
      Path basePath = ((FileSplit) split).getPath().getParent();
      return getDataStream(mapContext.getConfiguration(),
        basePath);
    } else {
      throw new IllegalArgumentException(
          "Could not ascertain BLOB base path from MapContext.");
    }
  }

  /**
   * Get access to the BLOB data itself.
   * This method returns an InputStream-based representation of the
   * BLOB data, accessing the filesystem for external BLOB storage
   * as necessary.
   * @param conf the Configuration used to access the filesystem
   * @param basePath the base directory where the table records are
   * stored.
   * @return an InputStream used to read the BLOB data.
   * @throws IOException if it could not read the BLOB from external storage.
   */
  public InputStream getDataStream(Configuration conf, Path basePath)
      throws IOException {
    if (isExternal()) {
      // use external storage.
      FileSystem fs = FileSystem.get(conf);
      return fs.open(new Path(basePath, fileName));
    } else {
      return new ByteArrayInputStream(data.getBytes());
    }
  }


  public byte [] getData() {
    if (isExternal()) {
      throw new RuntimeException(
          "External BLOBs must be read via getDataStream()");
    }

    return data.getBytes();
  }

  @Override
  public String toString() {
    if (isExternal()) {
      return "externalBlob(" + fileName + ")";
    } else {
      return data.toString();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // The serialization format for this object is:
    // boolean isExternal
    // if true, the next field is a String containing the file name.
    // if false, the next field is a BytesWritable containing the
    // actual data.

    boolean isExternal = in.readBoolean();
    if (isExternal) {
      this.data = null;
      this.fileName = Text.readString(in);
    } else {
      if (null == this.data) {
        this.data = new BytesWritable();
      }
      this.data.readFields(in);
      this.fileName = null;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(isExternal());
    if (isExternal()) {
      Text.writeString(out, fileName);
    } else {
      data.write(out);
    }
  }

  private static final ThreadLocal<Matcher> EXTERNAL_MATCHER =
    new ThreadLocal<Matcher>() {
      @Override protected Matcher initialValue() {
        Pattern externalPattern = Pattern.compile("externalBlob\\((.*)\\)");
        return externalPattern.matcher("");
      }
    };

  /**
   * Create a BlobRef based on parsed data from a line of text.
   * This only operates correctly on external blobs; inline blobs are simply
   * returned as null. You should store BLOB data in SequenceFile format
   * if reparsing is necessary.
   * @param inputString the text-based input data to parse.
   * @return a new BlobRef containing a reference to an external BLOB, or
   * an empty BlobRef if the data to be parsed is actually inline.
   */
  public static BlobRef parse(String inputString) {
    // If inputString is of the form 'externalBlob(%s)', then this is an
    // external BLOB stored at the filename indicated by '%s'. Otherwise,
    // it is an inline BLOB, which we don't support parsing of.

    Matcher m = EXTERNAL_MATCHER.get();
    m.reset(inputString);
    if (m.matches()) {
      // Extract the filename component from the string.
      return new BlobRef(m.group(1));
    } else {
      // This is inline BLOB string data.
      LOG.warn(
          "Reparsing inline BLOB data is not supported; use SequenceFiles.");
      return new BlobRef();
    }
  }
}

