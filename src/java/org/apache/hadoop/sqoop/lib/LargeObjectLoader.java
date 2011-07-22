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

package org.apache.hadoop.sqoop.lib;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
public class LargeObjectLoader {

  // Currently, cap BLOB/CLOB objects at 16 MB until we can use external storage.
  public final static long DEFAULT_MAX_LOB_LENGTH = 16 * 1024 * 1024;

  public final static String MAX_INLINE_LOB_LEN_KEY =
      "sqoop.inline.lob.length.max";

  // The task context for the currently-initialized instance.
  private TaskInputOutputContext context;

  private FileSystem fs;

  // Counter that is used with the current task attempt id to
  // generate unique LOB file names.
  private long nextLobFileId = 0;

  public LargeObjectLoader(TaskInputOutputContext context)
      throws IOException {
    this.context = context;
    this.fs = FileSystem.get(context.getConfiguration());
  }

  /**
   * @return a filename to use to put an external LOB in.
   */
  private String getNextLobFileName() {
    String file = "_lob/obj_" + context.getConfiguration().get(
        JobContext.TASK_ID, "unknown_task_id") + nextLobFileId;
    nextLobFileId++;

    return file;
  }

  /**
   * Copies all character data from the provided Reader to the provided
   * Writer. Does not close handles when it's done.
   * @param reader data source
   * @param writer data sink
   * @throws IOException if an I/O error occurs either reading or writing.
   */
  private void copyAll(Reader reader, Writer writer) throws IOException {
    int bufferSize = context.getConfiguration().getInt("io.file.buffer.size",
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
    int bufferSize = context.getConfiguration().getInt("io.file.buffer.size",
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
  public BlobRef readBlobRef(int colNum, ResultSet r)
      throws IOException, InterruptedException, SQLException {

    long maxInlineLobLen = context.getConfiguration().getLong(
        MAX_INLINE_LOB_LEN_KEY,
        DEFAULT_MAX_LOB_LENGTH);

    Blob b = r.getBlob(colNum);
    if (null == b) {
      return null;
    } else if (b.length() > maxInlineLobLen) {
      // Deserialize very large BLOBs into separate files.
      String fileName = getNextLobFileName();
      Path p = new Path(FileOutputFormat.getWorkOutputPath(context), fileName);

      Path parent = p.getParent();
      if (!fs.exists(parent)) {
        fs.mkdirs(parent);
      }

      BufferedOutputStream bos = null;
      InputStream is = null;
      OutputStream os = fs.create(p);
      try {
        bos = new BufferedOutputStream(os);
        is = b.getBinaryStream();
        copyAll(is, bos);
      } finally {
        if (null != bos) {
          bos.close();
          os = null; // os is now closed.
        }

        if (null != os) {
          os.close();
        }

        if (null != is) {
          is.close();
        }
      }

      return new BlobRef(fileName);
    } else {
      // This is a 1-based array.
      return new BlobRef(b.getBytes(1, (int) b.length()));
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
  public ClobRef readClobRef(int colNum, ResultSet r)
      throws IOException, InterruptedException, SQLException {

    long maxInlineLobLen = context.getConfiguration().getLong(
        MAX_INLINE_LOB_LEN_KEY,
        DEFAULT_MAX_LOB_LENGTH);

    Clob c = r.getClob(colNum);
    if (null == c) {
      return null;
    } else if (c.length() > maxInlineLobLen) {
      // Deserialize large CLOB into separate file.
      String fileName = getNextLobFileName();
      Path p = new Path(FileOutputFormat.getWorkOutputPath(context), fileName);

      Path parent = p.getParent();
      if (!fs.exists(parent)) {
        fs.mkdirs(parent);
      }

      BufferedWriter w = null;
      Reader reader = null;
      OutputStream os = fs.create(p);
      try {
        w = new BufferedWriter(new OutputStreamWriter(os));
        reader = c.getCharacterStream();
        copyAll(reader, w);
      } finally {
        if (null != w) {
          w.close();
          os = null; // os is now closed.
        }

        if (null != os) {
          os.close();
        }

        if (null != reader) {
          reader.close();
        }
      }

      return new ClobRef(fileName, true);
    } else {
      // This is a 1-based array.
      return new ClobRef(c.getSubString(1, (int) c.length()));
    }
  }
}
