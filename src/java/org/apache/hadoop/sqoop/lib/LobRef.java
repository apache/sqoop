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

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.sqoop.io.LobFile;
import org.apache.hadoop.sqoop.io.LobReaderCache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Abstract base class that holds a reference to a Blob or a Clob.
 * DATATYPE is the type being held (e.g., a byte array).
 * CONTAINERTYPE is the type used to hold this data (e.g., BytesWritable).
 * ACCESSORTYPE is the type used to access this data in a streaming fashion
 *   (either an InputStream or a Reader).
 */
public abstract class LobRef<DATATYPE, CONTAINERTYPE, ACCESSORTYPE>
    implements Closeable, Writable {

  public static final Log LOG = LogFactory.getLog(LobRef.class.getName());

  protected LobRef() {
    this.fileName = null;
    this.offset = 0;
    this.length = 0;

    this.data = null;
  }

  protected LobRef(CONTAINERTYPE container) {
    this.fileName = null;
    this.offset = 0;
    this.length = 0;

    this.data = container;
  }

  protected LobRef(String file, long offset, long length) {
    this.fileName = file;
    this.offset = offset;
    this.length = length;

    this.data = null;
  }

  // If the data is 'small', it's held directly, here.
  protected CONTAINERTYPE data;

  // If there data is too large to materialize fully, it's written into a file
  // whose path (relative to the rest of the dataset) is recorded here. This
  // takes precedence if the value fof fileName is non-null. These records are
  // currently written into LobFile-formatted files, which hold multiple
  // records. The starting offset and length of the record are recorded here
  // as well.
  private String fileName;
  private long offset;
  private long length;

  // If we've opened a LobFile object, track our reference to it here.
  protected LobFile.Reader reader;

  @Override
  protected synchronized void finalize() throws Throwable {
    close();
  }

  public void close() throws IOException {
    // Discard any open LobReader.
    if (null != this.reader) {
      LobReaderCache.getCache().recycle(this.reader);
    }
  }

  /**
   * @return true if the LOB data is in an external file; false if
   * it materialized inline.
   */
  public boolean isExternal() {
    return fileName != null;
  }

  /**
   * Convenience method to access #getDataStream(Configuration, Path)
   * from within a map task that read this LobRef from a file-based
   * InputSplit.
   * @param mapContext the Mapper.Context instance that encapsulates
   * the current map task.
   * @return an object that lazily streams the record to the client.
   * @throws IllegalArgumentException if it cannot find the source
   * path for this LOB based on the MapContext.
   * @throws IOException if it could not read the LOB from external storage.
   */
  public ACCESSORTYPE getDataStream(Mapper.Context mapContext)
      throws IllegalArgumentException, IOException {
    InputSplit split = mapContext.getInputSplit();
    if (split instanceof FileSplit) {
      Path basePath = ((FileSplit) split).getPath().getParent();
      return getDataStream(mapContext.getConfiguration(),
        basePath);
    } else {
      throw new IllegalArgumentException(
          "Could not ascertain LOB base path from MapContext.");
    }
  }

  /**
   * Get access to the LOB data itself.
   * This method returns a lazy reader of the LOB data, accessing the
   * filesystem for external LOB storage as necessary.
   * @param conf the Configuration used to access the filesystem
   * @param basePath the base directory where the table records are
   * stored.
   * @return an object that lazily streams the record to the client.
   * @throws IOException if it could not read the LOB from external storage.
   */
  public ACCESSORTYPE getDataStream(Configuration conf, Path basePath)
      throws IOException {
    if (isExternal()) {
      // Read from external storage.
      Path pathToRead = LobReaderCache.qualify(
          new Path(basePath, fileName), conf);
      LOG.debug("Retreving data stream from external path: " + pathToRead);
      if (reader != null) {
        // We already have a reader open to a LobFile. Is it the correct file?
        if (!pathToRead.equals(reader.getPath())) {
          // No. Close this reader and get the correct one.
          LOG.debug("Releasing previous external reader for "
              + reader.getPath());
          LobReaderCache.getCache().recycle(reader);
          reader = LobReaderCache.getCache().get(pathToRead, conf);
        }
      } else {
        reader = LobReaderCache.getCache().get(pathToRead, conf);
      }

      // We now have a LobFile.Reader associated with the correct file. Get to
      // the correct offset and return an InputStream/Reader to the user.
      if (reader.tell() != offset) {
        LOG.debug("Seeking to record start offset " + offset);
        reader.seek(offset);
      }

      if (!reader.next()) {
        throw new IOException("Could not locate record at " + pathToRead
            + ":" + offset);
      }

      return getExternalSource(reader);
    } else {
      // This data is already materialized in memory; wrap it and return.
      return getInternalSource(data);
    }
  }

  /**
   * Using the LobFile reader, get an accessor InputStream or Reader to the
   * underlying data.
   */
  protected abstract ACCESSORTYPE getExternalSource(LobFile.Reader reader)
      throws IOException;

  /**
   * Wrap the materialized data in an InputStream or Reader.
   */
  protected abstract ACCESSORTYPE getInternalSource(CONTAINERTYPE data);

  /**
   * @return the materialized data itself.
   */
  protected abstract DATATYPE getInternalData(CONTAINERTYPE data);

  public DATATYPE getData() {
    if (isExternal()) {
      throw new RuntimeException(
          "External LOBs must be read via getDataStream()");
    }

    return getInternalData(data);
  }

  @Override
  public String toString() {
    if (isExternal()) {
      return "externalLob(lf," + fileName + "," + Long.toString(offset)
          + "," + Long.toString(length) + ")";
    } else {
      return data.toString();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // The serialization format for this object is:
    // boolean isExternal
    // if true, then:
    //   a string identifying the external storage type
    //   and external-storage-specific data.
    // if false, then we use readFieldsInternal() to allow BlobRef/ClobRef
    // to serialize as it sees fit.
    //
    // Currently the only external storage supported is LobFile, identified
    // by the string "lf". This serializes with the filename (as a string),
    // followed by a long-valued offset and a long-valued length.

    boolean isExternal = in.readBoolean();
    if (isExternal) {
      this.data = null;

      String storageType = Text.readString(in);
      if (!storageType.equals("lf")) {
        throw new IOException("Unsupported external LOB storage code: "
            + storageType);
      }

      // Storage type "lf" is LobFile: filename, offset, length.
      this.fileName = Text.readString(in);
      this.offset = in.readLong();
      this.length = in.readLong();
    } else {
      readFieldsInternal(in);

      this.fileName = null;
      this.offset = 0;
      this.length = 0;
    }
  }

  /**
   * Perform the readFields() operation on a fully-materializable record.
   * @param in the DataInput to deserialize from.
   */
  protected abstract void readFieldsInternal(DataInput in) throws IOException;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(isExternal());
    if (isExternal()) {
      Text.writeString(out, "lf"); // storage type "lf" for LobFile.
      Text.writeString(out, fileName);
      out.writeLong(offset);
      out.writeLong(length);
    } else {
      writeInternal(out);
    }
  }

  /**
   * Perform the write() operation on a fully-materializable record.
   * @param out the DataOutput to deserialize to.
   */
  protected abstract void writeInternal(DataOutput out) throws IOException;

  protected static final ThreadLocal<Matcher> EXTERNAL_MATCHER =
    new ThreadLocal<Matcher>() {
      @Override protected Matcher initialValue() {
        Pattern externalPattern = Pattern.compile(
            "externalLob\\(lf,(.*),([0-9]+),([0-9]+)\\)");
        return externalPattern.matcher("");
      }
    };
}

