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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.io.LobFile;
import com.cloudera.sqoop.util.TaskId;

/**
 * Contains a set of methods which can read db columns from a ResultSet into
 * Java types, and do serialization of these types to/from DataInput/DataOutput
 * for use with Hadoop's Writable implementation. This supports null values
 * for all types.
 *
 * This is a singleton instance class; only one may exist at a time.
 * However, its lifetime is limited to the current TaskInputOutputContext's
 * life.
 */
public class LargeObjectLoader implements Closeable  {

  // Spill to external storage for BLOB/CLOB objects > 16 MB.
  public static final long DEFAULT_MAX_LOB_LENGTH = 16 * 1024 * 1024;

  public static final String MAX_INLINE_LOB_LEN_KEY =
      "sqoop.inline.lob.length.max";

  private Configuration conf;
  private Path workPath;
  private FileSystem fs;

  // Handles to the open BLOB / CLOB file writers.
  private LobFile.Writer curBlobWriter;
  private LobFile.Writer curClobWriter;

  // Counter that is used with the current task attempt id to
  // generate unique LOB file names.
  private long nextLobFileId = 0;

  /**
   * Create a new LargeObjectLoader.
   * @param conf the Configuration to use
   */
  public LargeObjectLoader(Configuration conf, Path workPath)
      throws IOException {
    this.conf = conf;
    this.workPath = workPath;
    this.fs = FileSystem.get(conf);
    this.curBlobWriter = null;
    this.curClobWriter = null;
  }

  @Override
  protected synchronized void finalize() throws Throwable {
    close();
    super.finalize();
  }

  @Override
  public void close() throws IOException {
    if (null != curBlobWriter) {
      curBlobWriter.close();
      curBlobWriter = null;
    }

    if (null != curClobWriter) {
      curClobWriter.close();
      curClobWriter = null;
    }
  }

  /**
   * @return a filename to use to put an external LOB in.
   */
  private String getNextLobFileName() {
    String file = "_lob/large_obj_" + TaskId.get(conf, "unknown_task_id")
        + nextLobFileId + ".lob";
    nextLobFileId++;

    return file;
  }

  /**
   * Calculates a path to a new LobFile object, creating any
   * missing directories.
   * @return a Path to a LobFile to write
   */
  private Path getNextLobFilePath() throws IOException {
    Path p = new Path(workPath, getNextLobFileName());
    Path parent = p.getParent();
    if (!fs.exists(parent)) {
      fs.mkdirs(parent);
    }

    return p;
  }

  /**
   * @return the current LobFile writer for BLOBs, creating one if necessary.
   */
  private LobFile.Writer getBlobWriter() throws IOException {
    if (null == this.curBlobWriter) {
      this.curBlobWriter = LobFile.create(getNextLobFilePath(), conf, false);
    }

    return this.curBlobWriter;
  }

  /**
   * @return the current LobFile writer for CLOBs, creating one if necessary.
   */
  private LobFile.Writer getClobWriter() throws IOException {
    if (null == this.curClobWriter) {
      this.curClobWriter = LobFile.create(getNextLobFilePath(), conf, true);
    }

    return this.curClobWriter;
  }

  /**
   * Returns the path being written to by a given LobFile.Writer, relative
   * to the working directory of this LargeObjectLoader.
   * @param w the LobFile.Writer whose path should be examined.
   * @return the path this is writing to, relative to the current working dir.
   */
  private String getRelativePath(LobFile.Writer w) {
    Path writerPath = w.getPath();

    String writerPathStr = writerPath.toString();
    String workPathStr = workPath.toString();
    if (!workPathStr.endsWith(File.separator)) {
      workPathStr = workPathStr + File.separator;
    }

    if (writerPathStr.startsWith(workPathStr)) {
      return writerPathStr.substring(workPathStr.length());
    }

    // Outside the working dir; return the whole thing.
    return writerPathStr;
  }

  /**
   * Copies all character data from the provided Reader to the provided
   * Writer. Does not close handles when it's done.
   * @param reader data source
   * @param writer data sink
   * @throws IOException if an I/O error occurs either reading or writing.
   */
  private void copyAll(Reader reader, Writer writer) throws IOException {
    int bufferSize = conf.getInt("io.file.buffer.size",
        4096);
    char [] buf = new char[bufferSize];

    while (true) {
      int charsRead = reader.read(buf);
      if (-1 == charsRead) {
        break; // no more stream to read.
      }
      writer.write(buf, 0, charsRead);
    }
  }

  /**
   * Copies all byte data from the provided InputStream to the provided
   * OutputStream. Does not close handles when it's done.
   * @param input data source
   * @param output data sink
   * @throws IOException if an I/O error occurs either reading or writing.
   */
  private void copyAll(InputStream input, OutputStream output)
      throws IOException {
    int bufferSize = conf.getInt("io.file.buffer.size",
        4096);
    byte [] buf = new byte[bufferSize];

    while (true) {
      int bytesRead = input.read(buf, 0, bufferSize);
      if (-1 == bytesRead) {
        break; // no more stream to read.
      }
      output.write(buf, 0, bytesRead);
    }
  }

  /**
   * Actually read a BlobRef instance from the ResultSet and materialize
   * the data either inline or to a file.
   *
   * @param colNum the column of the ResultSet's current row to read.
   * @param r the ResultSet to read from.
   * @return a BlobRef encapsulating the data in this field.
   * @throws IOException if an error occurs writing to the FileSystem.
   * @throws SQLException if an error occurs reading from the database.
   */
  public com.cloudera.sqoop.lib.BlobRef readBlobRef(int colNum, ResultSet r)
      throws IOException, InterruptedException, SQLException {

    long maxInlineLobLen = conf.getLong(
        MAX_INLINE_LOB_LEN_KEY,
        DEFAULT_MAX_LOB_LENGTH);

    Blob b = r.getBlob(colNum);
    if (null == b) {
      return null;
    } else if (b.length() > maxInlineLobLen) {
      // Deserialize very large BLOBs into separate files.
      long len = b.length();
      LobFile.Writer lobWriter = getBlobWriter();

      long recordOffset = lobWriter.tell();
      InputStream is = null;
      OutputStream os = lobWriter.writeBlobRecord(len);
      try {
        is = b.getBinaryStream();
        copyAll(is, os);
      } finally {
        if (null != os) {
          os.close();
        }

        if (null != is) {
          is.close();
        }

        // Mark the record as finished.
        lobWriter.finishRecord();
      }

      return new com.cloudera.sqoop.lib.BlobRef(
          getRelativePath(curBlobWriter), recordOffset, len);
    } else {
      // This is a 1-based array.
      return new com.cloudera.sqoop.lib.BlobRef(
          b.getBytes(1, (int) b.length()));
    }
  }


  /**
   * Actually read a ClobRef instance from the ResultSet and materialize
   * the data either inline or to a file.
   *
   * @param colNum the column of the ResultSet's current row to read.
   * @param r the ResultSet to read from.
   * @return a ClobRef encapsulating the data in this field.
   * @throws IOException if an error occurs writing to the FileSystem.
   * @throws SQLException if an error occurs reading from the database.
   */
  public com.cloudera.sqoop.lib.ClobRef readClobRef(int colNum, ResultSet r)
      throws IOException, InterruptedException, SQLException {

    long maxInlineLobLen = conf.getLong(
        MAX_INLINE_LOB_LEN_KEY,
        DEFAULT_MAX_LOB_LENGTH);

    Clob c = r.getClob(colNum);
    if (null == c) {
      return null;
    } else if (c.length() > maxInlineLobLen) {
      // Deserialize large CLOB into separate file.
      long len = c.length();
      LobFile.Writer lobWriter = getClobWriter();

      long recordOffset = lobWriter.tell();
      Reader reader = null;
      Writer w = lobWriter.writeClobRecord(len);
      try {
        reader = c.getCharacterStream();
        copyAll(reader, w);
      } finally {
        if (null != w) {
          w.close();
        }

        if (null != reader) {
          reader.close();
        }

        // Mark the record as finished.
        lobWriter.finishRecord();
      }

      return new com.cloudera.sqoop.lib.ClobRef(
          getRelativePath(lobWriter), recordOffset, len);
    } else {
      // This is a 1-based array.
      return new com.cloudera.sqoop.lib.ClobRef(
          c.getSubString(1, (int) c.length()));
    }
  }
}
