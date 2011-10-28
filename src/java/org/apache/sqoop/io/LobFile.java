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
package org.apache.sqoop.io;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;

import com.cloudera.sqoop.io.LobReaderCache;
import com.cloudera.sqoop.util.RandomHash;

/**
 * File format which stores large object records.
 * The format allows large objects to be read through individual InputStreams
 * to allow reading without full materialization of a single record.
 * Each record is assigned an id and can be accessed by id efficiently by
 * consulting an index at the end of the file.
 *
 * The LobFile format is specified at:
 * http://wiki.github.com/cloudera/sqoop/sip-3
 */
public final class LobFile {

  public static final Log LOG = LogFactory.getLog(LobFile.class.getName());
  public static final int LATEST_LOB_VERSION = 0;

  public static final char[] HEADER_ID_STR = { 'L', 'O', 'B' };

  //Value for entryId to write to the beginning of an IndexSegment.
  public static final long SEGMENT_HEADER_ID = -1;

  //Value for entryId to write before the finale.
  public static final long SEGMENT_OFFSET_ID = -2;

  //Value for entryID to write before the IndexTable
  public static final long INDEX_TABLE_ID = -3;

  private LobFile() {
  }

  /**
   * Creates a LobFile Reader configured to read from the specified file.
   */
  public static com.cloudera.sqoop.io.LobFile.Reader
      open(Path p, Configuration conf) throws IOException {
    FileSystem fs = p.getFileSystem(conf);
    FileStatus [] stats = fs.listStatus(p);
    if (null == stats || stats.length == 0) {
      throw new IOException("Could not find file: " + p);
    }
    FSDataInputStream fis = fs.open(p);
    DataInputStream dis = new DataInputStream(fis);
    LobFileHeader header = new LobFileHeader(dis);
    int version = header.getVersion();

    if (version == 0) {
      return new V0Reader(p, conf, header, dis, fis, stats[0].getLen());
    } else {
      throw new IOException("No reader available for LobFile version "
          + version);
    }
  }

  /**
   * Creates a LobFile Writer.
   * @param p the path to create.
   * @param conf the configuration to use to interact with the filesystem.
   * @param isCharData true if this is for CLOBs, false for BLOBs.
   * @param codec the compression codec to use (or null for none).
   * @param entriesPerSegment number of entries per index segment.
   */
  public static com.cloudera.sqoop.io.LobFile.Writer
            create(Path p, Configuration conf, boolean isCharData,
            String codec, int entriesPerSegment)
      throws IOException {
    return new V0Writer(p, conf, isCharData, codec, entriesPerSegment);
  }

  /**
   * Creates a LobFile Writer.
   * @param p the path to create.
   * @param conf the configuration to use to interact with the filesystem.
   * @param isCharData true if this is for CLOBs, false for BLOBs.
   * @param codec the compression codec to use (or null for none).
   */
  public static com.cloudera.sqoop.io.LobFile.Writer
            create(Path p, Configuration conf, boolean isCharData,
            String codec) throws IOException {
    return create(p, conf, isCharData, codec,
        V0Writer.DEFAULT_MAX_SEGMENT_ENTRIES);
  }

  /**
   * Creates a LobFile Writer configured for uncompressed data.
   * @param p the path to create.
   * @param conf the configuration to use to interact with the filesystem.
   * @param isCharData true if this is for CLOBs, false for BLOBs.
   */
  public static com.cloudera.sqoop.io.LobFile.Writer
            create(Path p, Configuration conf, boolean isCharData)
      throws IOException {
    return create(p, conf, isCharData, null);
  }

  /**
   * Creates a LobFile Writer configured for uncompressed binary data.
   * @param p the path to create.
   * @param conf the configuration to use to interact with the filesystem.
   */
  public static com.cloudera.sqoop.io.LobFile.Writer
            create(Path p, Configuration conf) throws IOException {
    return create(p, conf, false);
  }

  /**
   * Class that writes out a LobFile. Instantiate via LobFile.create().
   */
  public abstract static class Writer implements Closeable {
    /**
     * If this Writer is writing to a physical LobFile, then this returns
     * the file path it is writing to. Otherwise it returns null.
     * @return the fully-qualified path being written to by this writer.
     */
    public abstract Path getPath();

    /**
     * Finishes writing the LobFile and closes underlying handles.
     */
    public abstract void close() throws IOException;

    @Override
    protected synchronized void finalize() throws Throwable {
      close();
      super.finalize();
    }

    /**
     * Terminates the current record and writes any trailing zero-padding
     * required by the specified record size.
     * This is implicitly called between consecutive writeBlobRecord() /
     * writeClobRecord() calls.
     */
    public abstract void finishRecord() throws IOException;

    /**
     * Declares a new BLOB record to be written to the file.
     * @param len the "claimed" number of bytes that will be written to
     * this record. The actual number of bytes may differ.
     */
    public abstract OutputStream writeBlobRecord(long len) throws IOException;

    /**
     * Declares a new CLOB record to be written to the file.
     * @param len the claimed number of characters that will be written to
     * this record. The actual number of characters may differ.
     */
    public abstract java.io.Writer writeClobRecord(long len)
        throws IOException;

    /**
     * Report the current position in the output file.
     * @return the number of bytes written through this Writer.
     */
    public abstract long tell() throws IOException;

    /**
     * Checks whether an underlying stream is present or null.
     * @param out the stream to check for null-ness.
     * @throws IOException if out is null.
     */
    protected void checkForNull(OutputStream out) throws IOException {
      if (null == out) {
        throw new IOException("Writer has been closed.");
      }
    }
  }

  /**
   * Class that can read a LobFile. Create with LobFile.open().
   */
  public abstract static class Reader implements Closeable {
    /**
     * If this Reader is reading from a physical LobFile, then this returns
     * the file path it is reading from. Otherwise it returns null.
     * @return the fully-qualified path being read by this reader.
     */
    public abstract Path getPath();

    /**
     * Report the current position in the file. Note that the internal
     * cursor may move in an unpredictable fashion; e.g., to fetch
     * additional data from the index stored at the end of the file.
     * Clients may be more interested in the getRecordOffset() method
     * which returns the starting offset of the current record.
     * @return the current offset from the start of the file in bytes.
     */
    public abstract long tell() throws IOException;

    /**
     * Move the file pointer to the first available full record beginning at
     * position 'pos', relative to the start of the file.  After calling
     * seek(), you will need to call next() to move to the record itself.
     * @param pos the position to seek to or past.
     */
    public abstract void seek(long pos) throws IOException;

    /**
     * Advances to the next record in the file.
     * @return true if another record exists, or false if the
     * end of the file has been reached.
     */
    public abstract boolean next() throws IOException;

    /**
     * @return true if we have aligned the Reader (through a call to next())
     * onto a record.
     */
    public abstract boolean isRecordAvailable();

    /**
     * Reports the length of the record to the user.
     * If next() has not been called, or seek() has been called without
     * a subsequent call to next(), or next() returned false, the return
     * value of this method is undefined.
     * @return the 'claimedLen' field of the current record. For
     * character-based records, this is often in characters, not bytes.
     * Records may have more bytes associated with them than are reported
     * by this method, but never fewer.
     */
    public abstract long getRecordLen();

    /**
     * Return the entryId of the current record to the user.
     * If next() has not been called, or seek() has been called without
     * a subsequent call to next(), or next() returned false, the return
     * value of this method is undefined.
     * @return the 'entryId' field of the current record.
     */
    public abstract long getRecordId();

    /**
     * Return the byte offset at which the current record starts.
     * If next() has not been called, or seek() has been called without
     * a subsequent call to next(), or next() returned false, the return
     * value of this method is undefined.
     * @return the byte offset of the beginning of the current record.
     */
    public abstract long getRecordOffset();

    /**
     * @return an InputStream allowing the user to read the next binary
     * record from the file.
     */
    public abstract InputStream readBlobRecord() throws IOException;

    /**
     * @return a java.io.Reader allowing the user to read the next character
     * record from the file.
     */
    public abstract java.io.Reader readClobRecord() throws IOException;

    /**
     * Closes the reader.
     */
    public abstract void close() throws IOException;

    /**
     * Checks whether an underlying stream is present or null.
     * @param in the stream to check for null-ness.
     * @throws IOException if in is null.
     */
    protected void checkForNull(InputStream in) throws IOException {
      if (null == in) {
        throw new IOException("Reader has been closed.");
      }
    }

    /**
     * @return true if the Reader.close() method has been called.
     */
    public abstract boolean isClosed();

    @Override
    protected synchronized void finalize() throws Throwable {
      close();
      super.finalize();
    }
  }

  /**
   * Represents a header block in a LobFile. Can write a new header
   * block (and generate a record start mark), or read an existing
   * header block.
   */
  private static class LobFileHeader implements Writable {

    private int version;
    private RecordStartMark startMark;
    private MetaBlock metaBlock;

    /**
     * Create a new LobFileHeader.
     */
    public LobFileHeader() {
      this.version = LATEST_LOB_VERSION;
      this.startMark = new RecordStartMark();
      this.metaBlock = new MetaBlock();
    }

    /**
     * Read a LobFileHeader from an existing file.
     */
    public LobFileHeader(DataInput in) throws IOException {
      readFields(in);
    }

    /**
     * Write a LobFile header to an output sink.
     */
    public void write(DataOutput out) throws IOException {
      // Start with the file type identification.
      for (char c : HEADER_ID_STR) {
        out.writeByte((int) c);
      }

      // Write the format version
      WritableUtils.writeVInt(out, this.version);

      startMark.write(out);
      metaBlock.write(out);
    }

    public void readFields(DataInput in) throws IOException {
      char [] chars = new char[3];
      for (int i = 0; i < 3; i++) {
        chars[i] = (char) in.readByte();
      }

      // Check that these match what we expect. Throws IOE if not.
      checkHeaderChars(chars);

      this.version = WritableUtils.readVInt(in);
      if (this.version != LATEST_LOB_VERSION) {
        // Right now we only have one version we can handle.
        throw new IOException("Unexpected LobFile version " + this.version);
      }

      this.startMark = new RecordStartMark(in);
      this.metaBlock = new MetaBlock(in);
    }

    /**
     * Checks that a header array matches the standard LobFile header.
     * Additional data at the end of the headerStamp is ignored.
     * @param headerStamp the header bytes received from the file.
     * @throws IOException if it doesn't.
     */
    private void checkHeaderChars(char [] headerStamp) throws IOException {
      if (headerStamp.length != HEADER_ID_STR.length) {
        throw new IOException("Invalid LobFile header stamp: expected length "
            + HEADER_ID_STR.length);
      }
      for (int i = 0; i < HEADER_ID_STR.length; i++) {
        if (headerStamp[i] != HEADER_ID_STR[i]) {
          throw new IOException("Invalid LobFile header stamp");
        }
      }
    }

    /**
     * @return the format version number for this LobFile
     */
    public int getVersion() {
      return version;
    }

    /**
     * @return the RecordStartMark for this LobFile.
     */
    public RecordStartMark getStartMark() {
      return startMark;
    }

    /**
     * @return the MetaBlock for this LobFile.
     */
    public MetaBlock getMetaBlock() {
      return metaBlock;
    }
  }

  /**
   * Holds a RecordStartMark -- a 16 byte randomly-generated
   * sync token. Can read a RSM from an input source, or can
   * generate a new one.
   */
  private static class RecordStartMark implements Writable {

    // This is a 16-byte array.
    public static final int START_MARK_LENGTH = 16;

    private byte [] startBytes;

    public RecordStartMark() {
      generateStartMark();
    }

    public RecordStartMark(DataInput in) throws IOException {
      readFields(in);
    }

    public byte [] getBytes() {
      byte [] out = new byte[START_MARK_LENGTH];
      System.arraycopy(this.startBytes, 0, out, 0, START_MARK_LENGTH);
      return out;
    }

    public void readFields(DataInput in) throws IOException {
      this.startBytes = new byte[START_MARK_LENGTH];
      in.readFully(this.startBytes);
    }

    public void write(DataOutput out) throws IOException {
      out.write(this.startBytes);
    }

    /**
     * Generate a new random RecordStartMark.
     */
    private void generateStartMark() {
      this.startBytes = RandomHash.generateMD5Bytes();
    }
  }

  /**
   * Represents the metadata block stored in the header of a LobFile.
   */
  private static class MetaBlock extends AbstractMap<String, BytesWritable>
      implements Writable {

    // Strings which typically appear in the metablock have canonical names.
    public static final String ENTRY_ENCODING_KEY = "EntryEncoding";
    public static final String COMPRESSION_CODEC_KEY = "CompressionCodec";
    public static final String ENTRIES_PER_SEGMENT_KEY = "EntriesPerSegment";

    // Standard entry encodings.
    public static final String CLOB_ENCODING = "CLOB";
    public static final String BLOB_ENCODING = "BLOB";

    private Map<String, BytesWritable> entries;

    public MetaBlock() {
      entries = new TreeMap<String, BytesWritable>();
    }

    public MetaBlock(DataInput in) throws IOException {
      entries = new TreeMap<String, BytesWritable>();
      readFields(in);
    }

    public MetaBlock(Map<String, BytesWritable> map) {
      entries = new TreeMap<String, BytesWritable>();
      for (Map.Entry<String, BytesWritable> entry : map.entrySet()) {
        entries.put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public Set<Map.Entry<String, BytesWritable>> entrySet() {
      return entries.entrySet();
    }

    @Override
    public BytesWritable put(String k, BytesWritable v) {
      BytesWritable old = entries.get(k);
      entries.put(k, v);
      return old;
    }

    public BytesWritable put(String k, String v) {
      try {
        return put(k, new BytesWritable(v.getBytes("UTF-8")));
      } catch (UnsupportedEncodingException uee) {
        // Shouldn't happen; UTF-8 is always supported.
        throw new RuntimeException(uee);
      }
    }

    @Override
    public BytesWritable get(Object k) {
      return entries.get(k);
    }

    public String getString(Object k) {
      BytesWritable bytes = get(k);
      if (null == bytes) {
        return null;
      } else {
        try {
          return new String(bytes.getBytes(), 0, bytes.getLength(), "UTF-8");
        } catch (UnsupportedEncodingException uee) {
          // Shouldn't happen; UTF-8 is always supported.
          throw new RuntimeException(uee);
        }
      }
    }

    public void readFields(DataInput in) throws IOException {
      int numEntries = WritableUtils.readVInt(in);
      entries.clear();
      for (int i = 0; i < numEntries; i++) {
        String key = Text.readString(in);
        BytesWritable val = new BytesWritable();
        val.readFields(in);
        entries.put(key, val);
      }
    }

    public void write(DataOutput out) throws IOException {
      int numEntries = entries.size();
      WritableUtils.writeVInt(out, numEntries);
      for (Map.Entry<String, BytesWritable> entry : entries.entrySet()) {
        Text.writeString(out, entry.getKey());
        entry.getValue().write(out);
      }
    }
  }


  /**
   * Describes an IndexSegment. This is one entry in the IndexTable. It
   * holds the physical location of the IndexSegment in the file, as well
   * as the range of entryIds and byte ranges corresponding to records
   * described by the index subset in the IndexSegment.
   */
  private static class IndexTableEntry implements Writable {
    private long segmentOffset;
    private long firstIndexId;
    private long firstIndexOffset;
    private long lastIndexOffset;

    public IndexTableEntry() {
    }

    public IndexTableEntry(DataInput in) throws IOException {
      readFields(in);
    }

    private void setSegmentOffset(long offset) {
      this.segmentOffset = offset;
    }

    private void setFirstIndexId(long id) {
      this.firstIndexId = id;
    }

    private void setFirstIndexOffset(long offset) {
      this.firstIndexOffset = offset;
    }

    private void setLastIndexOffset(long offset) {
      this.lastIndexOffset = offset;
    }

    public void write(DataOutput out) throws IOException {
      WritableUtils.writeVLong(out, segmentOffset);
      WritableUtils.writeVLong(out, firstIndexId);
      WritableUtils.writeVLong(out, firstIndexOffset);
      WritableUtils.writeVLong(out, lastIndexOffset);
    }

    public void readFields(DataInput in) throws IOException {
      segmentOffset = WritableUtils.readVLong(in);
      firstIndexId = WritableUtils.readVLong(in);
      firstIndexOffset = WritableUtils.readVLong(in);
      lastIndexOffset = WritableUtils.readVLong(in);
    }

    /**
     * @return the entryId of the first record indexed by this segment.
     */
    public long getFirstIndexId() {
      return this.firstIndexId;
    }

    /**
     * @return the offset of the first record indexed by this segment.
     */
    public long getFirstIndexOffset() {
      return this.firstIndexOffset;
    }

    /**
     * @return the offset of the last record indexed by this segment.
     */
    public long getLastIndexOffset() {
      return this.lastIndexOffset;
    }

    /**
     * @return the offset from the start of the file of the IndexSegment
     * data itself.
     */
    public long getSegmentOffset() {
      return this.segmentOffset;
    }

    /**
     * Inform whether the user's requested offset corresponds
     * to a record that starts in this IndexSegment. If this
     * returns true, the requested offset may actually be in
     * a previous IndexSegment.
     * @param off the offset of the start of a record to test.
     * @return true if the user's requested offset is in this
     * or a previous IndexSegment.
     */
    public boolean containsOffset(long off) {
      return off <= getLastIndexOffset();
    }
  }

  /**
   * Class that represents the IndexSegment entries in a LobIndex.
   */
  private static class IndexSegment implements Writable {

    // The main body of the IndexSegment: the record lengths
    // of all the records in the IndexSegment.
    private BytesWritable recordLenBytes;

    // The length of the previously recorded field (used when
    // generating an index). Intermediate state used in calculation
    // of the lastIndexOffset.
    private long prevLength;

    // Used to write VLong-encoded lengths into a temp
    // array, which are then copied into recordLenBytes.
    private DataOutputBuffer outputBuffer;

    // The IndexTableEntry that describes this IndexSegment in the IndexTable.
    private IndexTableEntry tableEntry;

    public IndexSegment(IndexTableEntry tableEntry) {
      this.recordLenBytes = new BytesWritable();
      this.outputBuffer = new DataOutputBuffer(10); // max VLong size.
      this.tableEntry = tableEntry;
    }

    /**
     * Read an IndexSegment from an existing file.
     */
    public IndexSegment(IndexTableEntry tableEntry, DataInput in)
        throws IOException {
      this.recordLenBytes = new BytesWritable();
      this.outputBuffer = new DataOutputBuffer(10);
      this.tableEntry = tableEntry;
      readFields(in);
    }

    /**
     * @return the IndexTableEntry describing this IndexSegment in the
     * IndexTable.
     */
    public IndexTableEntry getTableEntry() {
      return tableEntry;
    }

    /**
     * Add a recordLength to the recordLenBytes array.
     */
    public void addRecordLen(long recordLen) throws IOException {
      // Allocate space for the new bytes.
      int numBytes = WritableUtils.getVIntSize(recordLen);
      recordLenBytes.setSize(recordLenBytes.getLength() + numBytes);

      // Write the new bytes into a temporary buffer wrapped in a DataOutput.
      outputBuffer.reset();
      WritableUtils.writeVLong(outputBuffer, recordLen);

      // Then copy those new bytes into the end of the recordLenBytes array.
      System.arraycopy(outputBuffer.getData(), 0, recordLenBytes.getBytes(),
          recordLenBytes.getLength() - numBytes, numBytes);

      // Now that we've added a new recordLength to the array,
      // it's the last index. We need to calculate its offset.
      // This is based on how long the previous record was.
      this.tableEntry.setLastIndexOffset(
          this.tableEntry.getLastIndexOffset() + this.prevLength);

      // Save this record's length (unserialized) for calculating
      // lastIndexOffset for the next record.
      this.prevLength = recordLen;
    }

    public void write(DataOutput out) throws IOException {
      // Write the SEGMENT_HEADER_ID to distinguish this from a LobRecord.
      WritableUtils.writeVLong(out, SEGMENT_HEADER_ID);

      // The length of the main body of the segment is the length of the
      // data byte array.
      int segmentBytesLen = recordLenBytes.getLength();
      WritableUtils.writeVLong(out, segmentBytesLen);

      // Write the body of the segment.
      out.write(recordLenBytes.getBytes(), 0, segmentBytesLen);
    }

    public void readFields(DataInput in) throws IOException {
      // After the RecordStartMark, we expect to get a SEGMENT_HEADER_ID (-1).
      long segmentId = WritableUtils.readVLong(in);
      if (SEGMENT_HEADER_ID != segmentId) {
        throw new IOException("Expected segment header id " + SEGMENT_HEADER_ID
            + "; got " + segmentId);
      }

      // Get the length of the rest of the segment, in bytes.
      long length = WritableUtils.readVLong(in);

      // Now read the actual main byte array.
      if (length > Integer.MAX_VALUE) {
        throw new IOException("Unexpected oversize data array length: "
            + length);
      } else if (length < 0) {
        throw new IOException("Unexpected undersize data array length: "
            + length);
      }
      byte [] segmentData = new byte[(int) length];
      in.readFully(segmentData);
      recordLenBytes = new BytesWritable(segmentData);

      reset(); // Reset the iterator allowing the user to yield offset/lengths.
    }


    // The following methods are used by a Reader to walk through the index
    // segment and get data about the records described in this segment of
    // the index.

    private DataInputBuffer dataInputBuf;

    // The following two fields are advanced by the next() method.
    private long curOffset; // offset into the file of the current record.
    private long curLen; // length of the current record in bytes.

    // Used to allow rewindOnce() to go backwards a single position in the
    // iterator.
    private int prevInputBufPos; // prev offset into dataInputBuf.
    private long prevOffset;
    private long prevLen;

    /**
     * Resets the record index iterator.
     */
    public void reset() {
      this.dataInputBuf = null;
    }

    /**
     * Aligns the iteration capability to return info about the next
     * record in the IndexSegment. Must be called before the first
     * record.
     * @return true if there is another record described in this IndexSegment.
     */
    public boolean next() {
      this.prevOffset = this.curOffset;
      if (null == dataInputBuf) {
        // We need to set up the iterator; this is the first use.
        if (null == recordLenBytes) {
          return false; // We don't have any records?
        }

        this.dataInputBuf = new DataInputBuffer();
        this.dataInputBuf.reset(recordLenBytes.getBytes(),
            0, recordLenBytes.getLength());

        this.curOffset = this.tableEntry.getFirstIndexOffset();
        this.prevOffset = 0;
      } else {
        this.curOffset += this.curLen;
      }

      boolean available = dataInputBuf.getPosition() < dataInputBuf.getLength();
      if (available) {
        this.prevInputBufPos = dataInputBuf.getPosition();
        // Then read out the next record length.
        try {
          this.prevLen = this.curLen;
          this.curLen = WritableUtils.readVLong(dataInputBuf);
        } catch (IOException ioe) {
          // Shouldn't happen; data in DataInputBuffer is materialized.
          throw new RuntimeException(ioe);
        }
      }

      return available;
    }

    /**
     * Undoes a single call to next(). This cannot be called twice in a row;
     * before calling this again, next() must be called in the interim. This
     * makes a subsequent call to next() yield the same iterated values as the
     * previous call.
     */
    public void rewindOnce() {
      // Move the buffer backwards so we deserialize the same VLong with
      // the next call.
      if (prevInputBufPos == 0) {
        // We actually rewound the first next() in the iterator.
        // Just reset the iterator to the beginning. Otherwise we'll
        // backfill it with bogus data.
        reset();
      } else {
        // Use the normal codepath; move the serialization buffer
        // backwards and restores the previously yielded values.
        dataInputBuf.reset(recordLenBytes.getBytes(), prevInputBufPos,
            recordLenBytes.getLength() - prevInputBufPos);

        // And restore the previously-yielded values.
        this.curLen = this.prevLen;
        this.curOffset = this.prevOffset;
      }
    }

    /**
     * Returns the length of the current record.
     * You must call next() and it must return true before calling this method.
     * @return the length in bytes of the current record.
     */
    public long getCurRecordLen() {
      return curLen;
    }

    /**
     * Returns the offset of the current record from the beginning of the file.
     * You must call next() and it must return true before calling this method.
     * @return the offset in bytes from the beginning of the file for the
     * current record.
     */
    public long getCurRecordStart() {
      return curOffset;
    }
  }

  /**
   * Stores the locations and ranges indexed by each IndexSegment.
   */
  private static class IndexTable
      implements Iterable<IndexTableEntry>, Writable {
    private List<IndexTableEntry> tableEntries;

    public IndexTable() {
      tableEntries = new ArrayList<IndexTableEntry>();
    }

    public IndexTable(DataInput in) throws IOException {
      readFields(in);
    }

    public void readFields(DataInput in) throws IOException {
      long recordTypeId = WritableUtils.readVLong(in);
      if (recordTypeId != INDEX_TABLE_ID) {
        // We expected to read an IndexTable.
        throw new IOException("Expected IndexTable; got record with typeId="
            + recordTypeId);
      }

      int tableCount = WritableUtils.readVInt(in);

      tableEntries = new ArrayList<IndexTableEntry>(tableCount);
      for (int i = 0; i < tableCount; i++) {
        tableEntries.add(new IndexTableEntry(in));
      }
    }

    public void write(DataOutput out) throws IOException {
      // Start with the record type id.
      WritableUtils.writeVLong(out, INDEX_TABLE_ID);

      // Then the count of the records.
      WritableUtils.writeVInt(out, tableEntries.size());

      // Followed by the table itself.
      for (IndexTableEntry entry : tableEntries) {
        entry.write(out);
      }
    }

    public void add(IndexTableEntry entry) {
      tableEntries.add(entry);
    }

    public IndexTableEntry get(int i) {
      return tableEntries.get(i);
    }

    public int size() {
      return tableEntries.size();
    }

    public Iterator<IndexTableEntry> iterator() {
      return tableEntries.iterator();
    }
  }

  /**
   * Reader implementation for LobFile format version 0. Acquire with
   * LobFile.open().
   */
  private static class V0Reader extends com.cloudera.sqoop.io.LobFile.Reader {
    public static final Log LOG = LogFactory.getLog(
        V0Reader.class.getName());

    // Forward seeks of up to this size are performed by reading, not seeking.
    private static final long MAX_CONSUMPTION_WIDTH = 512 * 1024;

    private LobFileHeader header;

    private Configuration conf;

    // Codec to use to decompress the file.
    private CompressionCodec codec;
    private Decompressor decompressor;

    // Length of the entire file.
    private long fileLen;

    // State bit set to true after we've called next() and successfully
    // aligned on a record. If true, we can hand an InputStream back to
    // the user.
    private boolean isAligned;

    // After we've aligned on a record, this contains the record's
    // reported length. In the presence of compression, etc, this may
    // not represent its true length in the file.
    private long claimedRecordLen;

    // After we've aligned on a record, this contains its entryId.
    private long curEntryId;

    // After we've aligned on a record, this contains the offset of the
    // beginning of its RSM from the start of the file.
    private long curRecordOffset;

    // After we've aligned on a record, this contains the record's
    // true length from the index.
    private long indexRecordLen;

    // tmp buffer used to consume RecordStartMarks during alignment.
    private byte [] tmpRsmBuf;

    // The actual file stream itself, which we can move around (e.g. with
    // seeking).
    private FSDataInputStream underlyingInput;

    // The data deserializer we typically place on top of this.
    // If we use underlyingInput.seek(), then we instantiate a new
    // dataIn on top of it.
    private DataInputStream dataIn;

    // The user accesses the current record through a stream memoized here.
    // We retain a pointer here so that we can forcibly close the old
    // userInputStream when they want to align on the next record.
    private InputStream userInputStream;

    // The current index segment to read record lengths from.
    private IndexSegment curIndexSegment;

    // The offset into the indexTable of the curIndexSegment.
    private int curIndexSegmentId;

    // The IndexTable that provides fast pointers to the IndexSegments.
    private IndexTable indexTable;

    // The path being opened.
    private Path path;

    // Users should use LobFile.open() instead of directly calling this.
    V0Reader(Path path, Configuration conf, LobFileHeader header,
        DataInputStream dis, FSDataInputStream stream, long fileLen)
        throws IOException {
      this.path = LobReaderCache.qualify(path, conf);
      this.conf = conf;
      this.header = header;
      this.dataIn = dis;
      this.underlyingInput = stream;
      this.isAligned = false;
      this.tmpRsmBuf = new byte[RecordStartMark.START_MARK_LENGTH];
      this.fileLen = fileLen;
      LOG.debug("Opening LobFile path: " + path);
      openCodec();
      openIndex();
    }

    /**
     * If the user has specified a compression codec in the header metadata,
     * create an instance of it.
     */
    private void openCodec() throws IOException {
      String codecName = header.getMetaBlock().getString(
          MetaBlock.COMPRESSION_CODEC_KEY);
      if (null != codecName) {
        LOG.debug("Decompressing file with codec: " + codecName);
        this.codec = CodecMap.getCodec(codecName, conf);
        if (null != this.codec) {
          this.decompressor = codec.createDecompressor();
        }
      }
    }

    /**
     * Get the first index segment out of the file; determine
     * where that is by loading the index locator at the end of
     * the file.
     */
    private void openIndex() throws IOException {
      // Jump to the end of the file.
      // At the end of the file is a RSM followed by two VLongs;
      // the first of these is the value -2 (one byte) and the
      // second of these is the offset of the beginning of the index (up to
      // 9 bytes).
      internalSeek(fileLen - RecordStartMark.START_MARK_LENGTH - 10);

      byte [] finaleBuffer = new byte[RecordStartMark.START_MARK_LENGTH + 10];
      this.dataIn.readFully(finaleBuffer);

      // Figure out where in the finaleBuffer the RSM actually starts,
      // as the finale might not fully fill the finaleBuffer.
      int rsmStart = findRecordStartMark(finaleBuffer);
      if (-1 == rsmStart) {
        throw new IOException(
            "Corrupt file index; could not find index start offset.");
      }

      // Wrap a buffer around those two vlongs.
      int vlongStart = rsmStart + RecordStartMark.START_MARK_LENGTH;
      DataInputBuffer inBuf = new DataInputBuffer();
      inBuf.reset(finaleBuffer, vlongStart, finaleBuffer.length - vlongStart);

      long offsetMarker = WritableUtils.readVLong(inBuf);
      if (SEGMENT_OFFSET_ID != offsetMarker) {
        // This isn't the correct signature; we got an RSM ahead of some
        // other data.
        throw new IOException("Invalid segment offset id: " + offsetMarker);
      }

      // This will contain the position of the IndexTable.
      long indexTableStart = WritableUtils.readVLong(inBuf);
      LOG.debug("IndexTable begins at " + indexTableStart);

      readIndexTable(indexTableStart);

      // Set up to read records from the beginning of the file. This
      // starts with the first IndexSegment.
      curIndexSegmentId = 0;
      loadIndexSegment();

      // This has moved the file pointer all over but we don't need to
      // worry about resetting it now. The next() method will seek the
      // file pointer to the first record when the user is ready to
      // consume it.
    }

    /**
     * Load the entire IndexTable into memory and decode it.
     */
    private void readIndexTable(long indexTableOffset) throws IOException {
      internalSeek(indexTableOffset);

      // Read the RecordStartMark ahead of the IndexTable.
      this.dataIn.readFully(tmpRsmBuf);
      if (!matchesRsm(tmpRsmBuf)) {
        throw new IOException("Expected record start mark before IndexTable");
      }

      this.indexTable = new IndexTable(dataIn);
    }

    /**
     * Ingest the next IndexSegment.
     */
    private void readNextIndexSegment() throws IOException {
      this.curIndexSegmentId++;
      loadIndexSegment();
    }

    /**
     * Load curIndexSegment with the segment specified by curIndexSegmentId.
     * The file pointer will be moved to the position after this segment.
     * If the segment id does not exist, then the curIndexSegment will be
     * set to null.
     */
    private void loadIndexSegment() throws IOException {
      if (indexTable.size() <= curIndexSegmentId || curIndexSegmentId < 0) {
        // We've iterated past the last IndexSegment. Set this to null
        // and return; the next() method will then return false.
        this.curIndexSegment = null;
        return;
      }

      // Otherwise, seek to the segment and load it.
      IndexTableEntry tableEntry = indexTable.get(curIndexSegmentId);
      long segmentOffset = tableEntry.getSegmentOffset();
      internalSeek(segmentOffset);
      readPositionedIndexSegment();
    }

    /**
     * When the underlying stream is aligned on the RecordStartMark
     * ahead of an IndexSegment, read in the next IndexSegment.
     * After this method the curIndexSegment contains the next
     * IndexSegment to read in the file; if the entire index has been
     * read in this fastion, curIndexSegment will be null.
     */
    private void readPositionedIndexSegment() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reading index segment at " + tell());
      }

      // Read the RecordStartMark ahead of the IndexSegment.
      this.dataIn.readFully(tmpRsmBuf);
      if (!matchesRsm(tmpRsmBuf)) {
        throw new IOException("Expected record start mark before IndexSegment");
      }

      // Read the IndexSegment proper.
      this.curIndexSegment = new IndexSegment(
          this.indexTable.get(curIndexSegmentId), this.dataIn);
    }

    /**
     * @return true if the bytes in 'buf' starting at 'offset' match
     * the RecordStartMark.
     * @param rsm the RecordStartMark
     * @param buf the buffer to check
     * @param offset the offset into buf to begin checking.
     */
    private boolean matchesRsm(byte [] rsm, byte [] buf, int offset) {
      for (int i = 0; i < RecordStartMark.START_MARK_LENGTH; i++) {
        if (buf[i + offset] != rsm[i]) {
          return false; // Mismatch at position i.
        }
      }

      return true; // Matched the whole thing.
    }

    private boolean matchesRsm(byte [] buf, int offset) {
      return matchesRsm(this.header.getStartMark().getBytes(),
          buf, offset);
    }

    private boolean matchesRsm(byte [] buf) {
      return matchesRsm(buf, 0);
    }

    /**
     * @return the offset in 'buf' where a RecordStartMark begins, or -1
     * if the RecordStartMark is not present in the buffer.
     */
    private int findRecordStartMark(byte [] buf) {
      byte [] rsm = this.header.getStartMark().getBytes();

      for (int i = 0; i < buf.length; i++) {
        if (matchesRsm(rsm, buf, i)) {
          return i;
        }
      }

      return -1; // couldn't find it.
    }

    @Override
    /** {@inheritDoc} */
    public Path getPath() {
      return this.path;
    }

    @Override
    /** {@inheritDoc} */
    public long tell() throws IOException {
      checkForNull(this.underlyingInput);
      return this.underlyingInput.getPos();
    }

    @Override
    /** {@inheritDoc} */
    public void seek(long pos) throws IOException {
      closeUserStream();
      checkForNull(this.underlyingInput);
      this.isAligned = false;
      searchForRecord(pos);
    }

    /**
     * Search the index for the first record starting on or after 'start'.
     * @param start the offset in the file where we should start looking
     * for a record.
     */
    private void searchForRecord(long start) throws IOException {
      LOG.debug("Looking for the first record at/after offset " + start);

      // Scan through the IndexTable until we find the IndexSegment
      // that contains the offset.
      for (int i = 0; i < indexTable.size(); i++) {
        IndexTableEntry tableEntry = indexTable.get(i);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Checking index table entry for range: "
              + tableEntry.getFirstIndexOffset() + ", "
              + tableEntry.getLastIndexOffset());
        }

        if (tableEntry.containsOffset(start)) {
          // Seek to the IndexSegment associated with this tableEntry.
          curIndexSegmentId = i;
          loadIndexSegment();

          // Use this index segment. The record index iterator
          // is at the beginning of the IndexSegment, since we just
          // read it in.
          LOG.debug("Found matching index segment.");
          while (this.curIndexSegment.next()) {
            long curStart = this.curIndexSegment.getCurRecordStart();
            if (curStart >= start) {
              LOG.debug("Found seek target record with offset " + curStart);
              // This is the first record to meet this criterion.
              // Rewind the index iterator by one so that the next()
              // method will do the right thing. next() will also
              // take care of actually seeking to the correct position
              // in the file to read the record proper.
              this.curIndexSegment.rewindOnce();
              return;
            }
          }

          // If it wasn't actually in this IndexSegment, then we've
          // got a corrupt IndexTableEntry; the entry represented that
          // the segment ran longer than it actually does.
          throw new IOException("IndexTableEntry claims last offset of "
              + tableEntry.getLastIndexOffset()
              + " but IndexSegment ends early."
              + " The IndexTable appears corrupt.");
        }
      }

      // If we didn't return inside the loop, then we've searched the entire
      // file and it's not there. Advance the IndexSegment iterator to
      // the end of the road so that next() returns false.
      this.curIndexSegmentId = indexTable.size();
      loadIndexSegment();
    }

    /**
     * Read data from the stream and discard it.
     * @param numBytes number of bytes to read and discard.
     */
    private void consumeBytes(int numBytes) throws IOException {
      int remaining = numBytes;
      while (remaining > 0) {
        int received = dataIn.skipBytes(remaining);
        if (received < 1) {
          throw new IOException("Could not consume additional bytes");
        }
        remaining -= received;
      }
    }

    /**
     * Seek to position 'pos' (offset from start of file). If this
     * is nearby, actually just consume data from the underlying
     * stream rather than doing a real seek.
     * @param targetPos the position to seek to, expressed as an offset
     * from the start of the file.
     */
    private void internalSeek(long targetPos) throws IOException {
      long curPos = this.underlyingInput.getPos();
      LOG.debug("Internal seek: target=" + targetPos + "; cur=" + curPos);
      long distance = targetPos - curPos;
      if (targetPos == curPos) {
        LOG.debug("(no motion required)");
        return; // We're already there!
      } else if (targetPos > curPos && distance < MAX_CONSUMPTION_WIDTH) {
        // We're "close enough" that we should just read it.
        LOG.debug("Advancing by " + distance + " bytes.");
        consumeBytes((int) distance);
      } else {
        LOG.debug("Direct seek to target");
        this.underlyingInput.seek(targetPos);
        this.dataIn = new DataInputStream(this.underlyingInput);
      }
    }

    /**
     * Close any stream to an open record that was opened by a user.
     */
    private void closeUserStream() throws IOException {
      if (this.userInputStream != null) {
        this.userInputStream.close();
        this.userInputStream = null;
      }
    }

    @Override
    /** {@inheritDoc} */
    public boolean next() throws IOException {
      LOG.debug("Checking for next record");
      checkForNull(this.underlyingInput);
      // If the user has opened a record stream, it is now void.
      closeUserStream();
      this.isAligned = false; // false until proven true.

      // Get the position of the next record start.
      // Check the index: is there another record?
      if (null == curIndexSegment) {
        LOG.debug("Index is finished; false");
        return false; // No index remains. Ergo, no more records.
      }
      boolean moreInSegment = curIndexSegment.next();
      if (!moreInSegment) {
        // The current IndexSegment has been exhausted. Move to the next.
        LOG.debug("Loading next index segment.");
        readNextIndexSegment();
        if (null == curIndexSegment) {
          LOG.debug("Index is finished; false");
          return false; // No index; no records.
        }

        // Try again with the next IndexSegment.
        moreInSegment = curIndexSegment.next();
      }

      if (!moreInSegment) {
        // Nothing left in the last IndexSegment.
        LOG.debug("Last index segment is finished; false.");
        this.curIndexSegment = null;
        return false;
      }

      // Determine where the next record starts.
      this.indexRecordLen = this.curIndexSegment.getCurRecordLen();
      this.curRecordOffset = this.curIndexSegment.getCurRecordStart();

      LOG.debug("Next record starts at position: " + this.curRecordOffset
          + "; indexedLen=" + this.indexRecordLen);

      // Make sure we're at the target position.
      internalSeek(this.curRecordOffset);

      // We are now on top of the next record's RecordStartMark.
      // Consume the RSM and the record header.
      this.dataIn.readFully(this.tmpRsmBuf);
      if (!matchesRsm(tmpRsmBuf)) {
        // No rsm? No dice.
        throw new IOException("Index contains bogus offset.");
      }

      this.curEntryId = WritableUtils.readVLong(this.dataIn);
      if (this.curEntryId < 0) {
        // We've moved past the end of the records and started
        // trying to consume the index. This is the EOF from
        // the client's perspective.
        LOG.debug("Indexed position is itself an IndexSegment; false.");
        return false;
      }
      LOG.debug("Aligned on record id=" + this.curEntryId);

      this.claimedRecordLen = WritableUtils.readVLong(this.dataIn);
      LOG.debug("Record has claimed length " + this.claimedRecordLen);
      // We are now aligned on the start of the user's data.
      this.isAligned = true;
      return true;
    }

    @Override
    /** {@inheritDoc} */
    public boolean isRecordAvailable() {
      return this.isAligned;
    }

    @Override
    /** {@inheritDoc} */
    public long getRecordLen() {
      return this.claimedRecordLen;
    }

    @Override
    /** {@inheritDoc} */
    public long getRecordId() {
      return this.curEntryId;
    }

    @Override
    /** {@inheritDoc} */
    public long getRecordOffset() {
      return this.curRecordOffset;
    }

    @Override
    /** {@inheritDoc} */
    public InputStream readBlobRecord() throws IOException {
      if (!isRecordAvailable()) {
        // we're not currently aligned on a record-start.
        // Try to get the next one.
        if (!next()) {
          // No more records available.
          throw new EOFException("End of file reached.");
        }
      }

      // Ensure any previously-open user record stream is closed.
      closeUserStream();

      // Mark this record as consumed.
      this.isAligned = false;

      // The length of the stream we can return to the user is
      // the indexRecordLen minus the length of any per-record headers.
      // That includes the RecordStartMark, the entryId, and the claimedLen.
      long streamLen = this.indexRecordLen - RecordStartMark.START_MARK_LENGTH
          - WritableUtils.getVIntSize(this.curEntryId)
          - WritableUtils.getVIntSize(this.claimedRecordLen);
      LOG.debug("Yielding stream to user with length " + streamLen);
      this.userInputStream = new FixedLengthInputStream(this.dataIn, streamLen);
      if (this.codec != null) {
        // The user needs to decompress the data; wrap the InputStream.
        decompressor.reset();
        this.userInputStream = new DecompressorStream(
            this.userInputStream, decompressor);
      }
      return this.userInputStream;
    }

    @Override
    /** {@inheritDoc} */
    public java.io.Reader readClobRecord() throws IOException {
      // Get a handle to the binary reader and then wrap it.
      InputStream is = readBlobRecord();
      return new InputStreamReader(is);
    }

    @Override
    /** {@inheritDoc} */
    public void close() throws IOException {
      closeUserStream();

      if (null != dataIn) {
        dataIn.close();
        dataIn = null;
      }

      if (null != underlyingInput) {
        underlyingInput.close();
        underlyingInput = null;
      }

      this.isAligned = false;
    }

    @Override
    /** {@inheritDoc} */
    public boolean isClosed() {
      return this.underlyingInput == null;
    }
  }


  /**
   * Concrete writer implementation for LobFile format version 0.
   * Instantiate via LobFile.create().
   */
  private static class V0Writer extends com.cloudera.sqoop.io.LobFile.Writer {
    public static final Log LOG = LogFactory.getLog(
        V0Writer.class.getName());

    private Configuration conf;
    private Path path;
    private boolean isCharData;
    private LobFileHeader header;

    private String codecName;
    private CompressionCodec codec;
    private Compressor compressor;

    // The LobIndex we are constructing.
    private LinkedList<IndexSegment> indexSegments;
    // Number of entries in the current IndexSegment.
    private int entriesInSegment;
    private IndexTable indexTable;

    // Number of entries that can be written to a single IndexSegment.
    private int maxEntriesPerSegment;

    // By default we write this many entries per IndexSegment.
    static final int DEFAULT_MAX_SEGMENT_ENTRIES = 4096;

    // Our OutputStream to the underlying file.
    private DataOutputStream out;

    // 'out' is layered on top of this stream, which gives us a count
    // of how much data we've written so far.
    private CountingOutputStream countingOut;

    // State regarding the current record being written.
    private long curEntryId; // entryId of the current LOB being written.
    private long curClaimedLen; // The user claims a length for a record.

    // The user's OutputStream and/or Writer that writes to us.
    private OutputStream userOutputStream;
    private java.io.Writer userWriter;

    // The userCountingOutputStream may be the same as userOutputStream;
    // but if the user is writing through a compressor, it is actually
    // underneath of it. This tells us how many compressed bytes were
    // really written.
    private CountingOutputStream userCountingOutputStream;

    /**
     * Creates a LobFile Writer for file format version 0.
     * @param p the path to create.
     * @param conf the configuration to use to interact with the filesystem.
     * @param isCharData true if this is for CLOBs, false for BLOBs.
     * @param codecName the compression codec to use (or null for none).
     * @param entriesPerSegment the number of index entries per IndexSegment.
     */
    V0Writer(Path p, Configuration conf, boolean isCharData,
        String codecName, int entriesPerSegment) throws IOException {

      this.path = LobReaderCache.qualify(p, conf);
      this.conf = conf;
      this.isCharData = isCharData;
      this.header = new LobFileHeader();
      this.indexSegments = new LinkedList<IndexSegment>();
      this.indexTable = new IndexTable();
      this.maxEntriesPerSegment = entriesPerSegment;

      this.codecName = codecName;
      if (this.codecName != null) {
        this.codec = CodecMap.getCodec(codecName, conf);
        if (null != this.codec) {
          this.compressor = codec.createCompressor();
        }
      }

      init();
    }

    /**
     * Open the file and write its header.
     */
    private void init() throws IOException {
      FileSystem fs = this.path.getFileSystem(conf);
      FSDataOutputStream fsOut = fs.create(this.path);
      this.countingOut = new CountingOutputStream(
          new BufferedOutputStream(fsOut));
      this.out = new DataOutputStream(this.countingOut);

      // put any necessary config strings into the header.
      MetaBlock m = this.header.getMetaBlock();
      if (isCharData) {
        m.put(MetaBlock.ENTRY_ENCODING_KEY, MetaBlock.CLOB_ENCODING);
      } else {
        m.put(MetaBlock.ENTRY_ENCODING_KEY, MetaBlock.BLOB_ENCODING);
      }

      if (null != codec) {
        m.put(MetaBlock.COMPRESSION_CODEC_KEY, this.codecName);
      }

      // Serialize the value of maxEntriesPerSegment as a VInt in a byte array
      // and put that into the metablock as ENTRIES_PER_SEGMENT_KEY.
      int segmentBufLen = WritableUtils.getVIntSize(this.maxEntriesPerSegment);
      DataOutputBuffer entriesPerSegBuf = new DataOutputBuffer(segmentBufLen);
      WritableUtils.writeVInt(entriesPerSegBuf, this.maxEntriesPerSegment);
      byte [] entriesPerSegArray =
          Arrays.copyOf(entriesPerSegBuf.getData(), segmentBufLen);
      m.put(MetaBlock.ENTRIES_PER_SEGMENT_KEY,
          new BytesWritable(entriesPerSegArray));

      // Write the file header to the file.
      this.header.write(out);

      // Now we're ready to accept record data from the user.
    }

    @Override
    /** {@inheritDoc} */
    public Path getPath() {
      return this.path;
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public long tell() throws IOException {
      checkForNull(this.out);
      this.out.flush();
      return this.countingOut.getByteCount();
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public void close() throws IOException {
      finishRecord();
      writeIndex();
      if (this.out != null) {
        this.out.close();
        this.out = null;
      }

      if (this.countingOut != null) {
        this.countingOut.close();
        this.countingOut = null;
      }
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public void finishRecord() throws IOException {
      if (null != this.userWriter) {
        this.userWriter.close();
        this.userWriter = null;
      }

      if (null != this.userCountingOutputStream) {

        // If there is a wrapping stream for compression,
        // close this first.
        if (null != this.userOutputStream
            && this.userOutputStream != this.userCountingOutputStream) {
          this.userOutputStream.close();
        }

        // Now close the "main" stream.
        this.userCountingOutputStream.close();

        // Write the true length of the current record to the index.
        updateIndex(this.userCountingOutputStream.getByteCount()
            + RecordStartMark.START_MARK_LENGTH
            + WritableUtils.getVIntSize(curEntryId)
            + WritableUtils.getVIntSize(curClaimedLen));

        this.userOutputStream = null;
        this.userCountingOutputStream = null;
      }

      if (null != this.out) {
        out.flush();
      }
    }

    /**
     * Write in the current IndexSegment, the true compressed length of the
     * record we just finished writing.
     * @param curRecordLen the true length in bytes of the compressed record.
     */
    private void updateIndex(long curRecordLen) throws IOException {
      LOG.debug("Adding index entry: id=" + curEntryId
          + "; len=" + curRecordLen);
      indexSegments.getLast().addRecordLen(curRecordLen);
      entriesInSegment++;
      curEntryId++;
    }

    /**
     * Write the index itself to the file.
     */
    private void writeIndex() throws IOException {

      // Write out all the segments in turn.
      // As we do so, reify their offsets into the IndexTable.
      for (IndexSegment segment : indexSegments) {
        long segmentOffset = tell();
        segment.getTableEntry().setSegmentOffset(segmentOffset);

        header.getStartMark().write(out);
        segment.write(out);
      }

      long indexTableStartPos = tell(); // Save for the end of the file.
      LOG.debug("IndexTable offset: " + indexTableStartPos);

      header.getStartMark().write(out);
      indexTable.write(out); // write the IndexTable record.

      // Write the finale that tells us where the IndexTable begins.
      header.getStartMark().write(out);
      WritableUtils.writeVLong(out, SEGMENT_OFFSET_ID);
      WritableUtils.writeVLong(out, indexTableStartPos);
    }

    /**
     * Prepare to index a new record that will soon be written to the file.
     * If this is is the first record in the current IndexSegment, we need
     * to record its entryId and the current file position.
     */
    private void startRecordIndex() throws IOException {
      if (entriesInSegment == maxEntriesPerSegment
          || indexSegments.size() == 0) {
        // The current segment is full. Start a new one.
        this.entriesInSegment = 0;
        IndexTableEntry tableEntry = new IndexTableEntry();
        IndexSegment curSegment = new IndexSegment(tableEntry);
        this.indexSegments.add(curSegment);

        long filePos = tell();
        LOG.debug("Starting IndexSegment; first id=" + curEntryId
            + "; off=" + filePos);
        tableEntry.setFirstIndexId(curEntryId);
        tableEntry.setFirstIndexOffset(filePos);
        tableEntry.setLastIndexOffset(filePos);
        this.indexTable.add(tableEntry);
      }
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public OutputStream writeBlobRecord(long claimedLen) throws IOException {
      finishRecord(); // finish any previous record.
      checkForNull(this.out);
      startRecordIndex();
      this.header.getStartMark().write(out);
      LOG.debug("Starting new record; id=" + curEntryId
          + "; claimedLen=" + claimedLen);
      WritableUtils.writeVLong(out, curEntryId);
      WritableUtils.writeVLong(out, claimedLen);
      this.curClaimedLen = claimedLen;
      this.userCountingOutputStream = new CountingOutputStream(
          new CloseShieldOutputStream(out));
      if (null == this.codec) {
        // No codec; pass thru the same OutputStream to the user.
        this.userOutputStream = this.userCountingOutputStream;
      } else {
        // Wrap our CountingOutputStream in a compressing OutputStream to
        // give to the user.
        this.compressor.reset();
        this.userOutputStream = new CompressorStream(
            this.userCountingOutputStream, compressor);
      }

      return this.userOutputStream;
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public java.io.Writer writeClobRecord(long len) throws IOException {
      if (!isCharData) {
        throw new IOException(
            "Can only write CLOB data to a Clob-specific LobFile");
      }

      // Get a binary handle to the record and wrap it in a java.io.Writer.
      writeBlobRecord(len);
      this.userWriter = new OutputStreamWriter(userOutputStream);
      return this.userWriter;
    }
  }
}
