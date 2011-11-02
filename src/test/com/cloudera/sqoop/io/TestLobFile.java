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

package com.cloudera.sqoop.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.CharBuffer;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Test the LobFile reader/writer implementation.
 */
public class TestLobFile extends TestCase {

  public static final Log LOG = LogFactory.getLog(
        TestLobFile.class.getName());

  public static final Path TEMP_BASE_DIR;

  static {
    String tmpDir = System.getProperty("test.build.data", "/tmp/");
    if (!tmpDir.endsWith(File.separator)) {
      tmpDir = tmpDir + File.separator;
    }

    TEMP_BASE_DIR = new Path(new Path(tmpDir), "lobtest");
  }

  private Configuration conf;
  private FileSystem fs;

  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");

    fs = FileSystem.getLocal(conf);
    fs.mkdirs(TEMP_BASE_DIR);
  }

  private long [] writeClobFile(Path p, String codec,
      String... records) throws Exception {
    if (fs.exists(p)) {
      fs.delete(p, false);
    }

    // memorize the offsets of each record we write.
    long [] offsets = new long[records.length];

    // Create files with four entries per index segment.
    LobFile.Writer writer = LobFile.create(p, conf, true, codec, 4);

    int i = 0;
    for (String r : records) {
      offsets[i++] = writer.tell();
      Writer w = writer.writeClobRecord(r.length());
      w.write(r);
      w.close();
    }

    writer.close();
    return offsets;
  }

  private void verifyClobFile(Path p, String... expectedRecords)
      throws Exception {

    LobFile.Reader reader = LobFile.open(p, conf);

    int recNum = 0;

    while (reader.next()) {
      // We should have a record of the same length as the expected one.
      String expected = expectedRecords[recNum];
      assertTrue(reader.isRecordAvailable());
      assertEquals(expected.length(), reader.getRecordLen());
      Reader r = reader.readClobRecord();

      // Read in the record and assert that we got enough characters out.
      CharBuffer buf = CharBuffer.allocate(expected.length());
      int bytesRead = 0;
      while (bytesRead < expected.length()) {
        int thisRead = r.read(buf);
        LOG.info("performed read of " + thisRead + " chars");
        if (-1 == thisRead) {
          break;
        }

        bytesRead += thisRead;
      }

      LOG.info("Got record of " + bytesRead + " chars");
      assertEquals(expected.length(), bytesRead);
      char [] charData = buf.array();
      String finalRecord = new String(charData);
      assertEquals(expected, finalRecord);

      recNum++;
    }

    // Check that we got everything.
    assertEquals(expectedRecords.length, recNum);

    reader.close();

    try {
      reader.next();
      fail("Expected IOException calling next after close");
    } catch (IOException ioe) {
      // expected this.
    }

    // A second close shouldn't hurt anything. This should be a no-op.
    reader.close();
  }

  private void runClobFileTest(Path p, String codec,
      String... records) throws Exception {
    writeClobFile(p, codec, records);
    verifyClobFile(p, records);
    fs.delete(p, false);
  }

  public void testEmptyRecord() throws Exception {
    runClobFileTest(new Path(TEMP_BASE_DIR, "empty.lob"), null);
  }

  public void testSingleRecord() throws Exception {
    runClobFileTest(new Path(TEMP_BASE_DIR, "single.lob"),
        null, "this is a single record!");
  }

  public void testMultiRecords() throws Exception {
    runClobFileTest(new Path(TEMP_BASE_DIR, "multi.lob"),
        CodecMap.NONE,
        "this is the first record",
        "this is the second record. I assure you that this record is long.",
        "yet one more record graces this file.");
  }

  public void testMultiIndexSegments() throws Exception {
    // Test that we can use multiple IndexSegments.
    runClobFileTest(new Path(TEMP_BASE_DIR, "multi-index.lob"),
        null,
        "this is the first record",
        "this is the second record. I assure you that this record is long.",
        "record number three",
        "last one in first index segment",
        "first in the second index segment",
        "yet one more record graces this file.");
  }

  /**
   * Run a test where we read only a fraction of the first record,
   * but then read the second record completely. Verify that we
   * can re-align on a record boundary correctly. This test requires
   * at least 3 records.
   * @param p the path to the file to create.
   * @param firstLine the first line of the first reord
   * @param records All of the records to write to the file.
   */
  private void runLineAndRecordTest(Path p, String firstLine,
      String... records) throws Exception {

    assertTrue("This test requires 3+ records", records.length > 2);

    writeClobFile(p, null, records);

    LobFile.Reader reader = LobFile.open(p, conf);

    // We should not yet be aligned.
    assertFalse(reader.isRecordAvailable());
    assertTrue(reader.next());
    // Now we should be.
    assertTrue(reader.isRecordAvailable());

    // Read just one line from the record.
    Reader r = reader.readClobRecord();
    BufferedReader br = new BufferedReader(r);
    String line = br.readLine();
    assertEquals(firstLine, line);

    br.close();
    r.close();

    // We should no longer be aligned on a record start.
    assertFalse(reader.isRecordAvailable());

    // We should now be able to get to record two.
    assertTrue(reader.next());

    // This should be nicely aligned even if the first record was not
    // completely consumed by a client.
    r = reader.readClobRecord();
    CharBuffer buf = CharBuffer.allocate(records[1].length());
    r.read(buf);
    r.close();
    char [] chars = buf.array();
    String s = new String(chars);
    assertEquals(records[1], s);

    // Close the reader before we consume the entire file.
    reader.close();
    assertFalse(reader.isRecordAvailable());
  }

  public void testVeryShortRead() throws Exception {
    // Read only a small fraction of a record, ensure that we can
    // read the next record, even when we've left more than a 16-byte
    // quantity in the readahead buffer.

    Path p = new Path(TEMP_BASE_DIR, "shortread.lob");
    final String FIRST_LINE = "line1";
    final String SECOND_LINE =
        "This contains much more in the record than just one line.";
    final String RECORD2 = "here is the second record.";
    final String RECORD3 = "The 3rd record, which we won't actually read.";

    runLineAndRecordTest(p, FIRST_LINE,
        FIRST_LINE + "\n" + SECOND_LINE,
        RECORD2,
        RECORD3);

  }

  public void testIncompleteOverread() throws Exception {
    // Read most of the first record so that we partially consume the
    // next record start mark; make sure we realign properly.

    Path p = new Path(TEMP_BASE_DIR, "longread.lob");
    final String FIRST_LINE = "this is a really long line of text to read!";
    final String SECOND_LINE = "not this.";
    final String RECORD2 = "Here is yet another record to verify.";
    final String RECORD3 = "Nobody cares about record 3.";

    runLineAndRecordTest(p, FIRST_LINE,
        FIRST_LINE + "\n" + SECOND_LINE,
        RECORD2,
        RECORD3);
  }

  public void testSeekToRecord() throws Exception {
    // Seek past the first two records and read the third.

    Path p = new Path(TEMP_BASE_DIR, "seek.lob");
    String [] records = {
      "this is the first record!",
      "here comes record number two. It is a bit longer.",
      "this is the third record. we can read it.",
    };

    // Write the file and memorize when the third record starts.
    LobFile.Writer writer = LobFile.create(p, conf, true);

    int recNum = 0;
    long rec3Start = 0;
    for (String r : records) {
      Writer w = writer.writeClobRecord(r.length());
      w.write(r);
      w.close();
      writer.finishRecord();
      if (recNum == 1) {
        rec3Start = writer.tell();
        LOG.info("Record three start: " + rec3Start);
      }
      recNum++;
    }

    writer.close();

    // Now reopen the file for read, seek to the third record, and get it.
    LobFile.Reader reader = LobFile.open(p, conf);
    reader.seek(rec3Start);
    assertTrue(reader.next());
    assertTrue(reader.isRecordAvailable());
    assertEquals(rec3Start, reader.getRecordOffset());

    Reader r = reader.readClobRecord();
    CharBuffer buf = CharBuffer.allocate(records[2].length());
    r.read(buf);
    r.close();
    char [] chars = buf.array();
    String s = new String(chars);
    assertEquals(records[2], s);

    r.close();
    reader.close();
  }


  /** Verifies that the next record in the LobFile is the expected one. */
  private void verifyNextRecord(LobFile.Reader reader, long expectedId,
      String expectedRecord) throws Exception {
    assertTrue(reader.next());
    assertTrue(reader.isRecordAvailable());
    assertEquals(expectedId, reader.getRecordId());

    Reader r = reader.readClobRecord();
    CharBuffer buf = CharBuffer.allocate(expectedRecord.length());
    int bytesRead = 0;
    while (bytesRead < expectedRecord.length()) {
      int thisRead = r.read(buf);
      if (-1 == thisRead) {
        break;
      }

      bytesRead += thisRead;
    }

    LOG.info("Got record of " + bytesRead + " chars");
    assertEquals(expectedRecord.length(), bytesRead);

    char [] charData = buf.array();
    String finalRecord = new String(charData);
    assertEquals(expectedRecord, finalRecord);
  }

  public void testManySeeks() throws Exception {
    // Test that we can do gymnastics with seeking between records.

    Path p = new Path(TEMP_BASE_DIR, "manyseeks.lob");

    String [] records = {
      "first record",
      "second record",
      "the third record",
      "rec4 is the last in IndexSeg 0",
      "rec5 is first in IndexSeg 1",
      "rec6 is yet another record",
      "rec7 is starting to feel boring",
      "rec8 is at the end of seg 1",
      "rec9 is all by itself in seg 2",
    };

    // Write the records to a file, save their offsets.
    long [] offsets = writeClobFile(p, null, records);

    // Sanity check that we can stream the file.
    verifyClobFile(p, records);

    // Open a handle to the file.
    LobFile.Reader reader = LobFile.open(p, conf);

    // Seeking to offset 0 should return the first record.
    reader.seek(0);
    verifyNextRecord(reader, 0, records[0]);

    // Seek to the last item in the first IndexSegment.
    reader.seek(offsets[3]);
    verifyNextRecord(reader, 3, records[3]);

    // Seek to just ahead of that same record.
    reader.seek(offsets[3] - 10);
    verifyNextRecord(reader, 3, records[3]);

    // Seek (backwards) to the first record.
    reader.seek(offsets[0]);
    verifyNextRecord(reader, 0, records[0]);

    // Seek to first record in second IndexSegment.
    reader.seek(offsets[4]);
    verifyNextRecord(reader, 4, records[4]);

    // Move backwards.
    reader.seek(0);

    // Seek to "no man's land" between last offset in first IndexSeg
    // and the first offset in second IndexSegment. Result should be
    // the first record in second InexSegment.
    reader.seek(offsets[4] - 10);
    verifyNextRecord(reader, 4, records[4]);

    // Seek to past the last record. No record should be returned.
    reader.seek(offsets[8] + 4);
    assertFalse("Found a record past last record start.", reader.next());

    // Seek to somewhere in the middle of IndexSegment 0.
    // This should recover just fine.
    reader.seek(offsets[2]);
    verifyNextRecord(reader, 2, records[2]);

    // Seek to last record in IndexSegment 1.
    reader.seek(offsets[3] - 1);
    verifyNextRecord(reader, 3, records[3]);

    // And make sure that iteration picks up naturally from there.
    verifyNextRecord(reader, 4, records[4]);

    // Seek well past the end of the file. No record should be returned.
    reader.seek(50000000);
    assertFalse("Found a record past expected end-of-file", reader.next());

    // Seek to somewhere in the index.
    reader.seek(offsets[8] + 32);
    assertFalse("Found a record past beginning of index", reader.next());

    // Seek to the last record (exact hit). This is a singleton IndexSegment.
    reader.seek(offsets[8]);
    verifyNextRecord(reader, 8, records[8]);

    // Seek to no-man's-land ahead of last record.
    reader.seek(offsets[8] - 3);
    verifyNextRecord(reader, 8, records[8]);

    reader.close();
  }

  /**
   * Verifies that a record to be read from a lob file has
   * as many bytes as we expect, and that the bytes are what we
   * expect them to be. Assumes that the bytes are such that
   * input[i] == i + offset.
   * @param reader the LobFile reader to consume data from
   * @param expectedDeclaredLen the size we expect the LobFile to declare
   * its record length as.
   * @param expectedActualLen the true number of bytes we expect to read in
   * the record.
   * @param offset the offset amount for each of the elements of the array.
   */
  private void verifyBlobRecord(LobFile.Reader reader,
      long expectedDeclaredLen, long expectedActualLen,
      int offset) throws Exception {

    assertTrue(reader.next());
    assertTrue(reader.isRecordAvailable());
    assertEquals(expectedDeclaredLen, reader.getRecordLen());

    InputStream is = reader.readBlobRecord();

    byte [] bytes = new byte[(int) expectedActualLen];
    int numRead = is.read(bytes);
    assertEquals(expectedActualLen, numRead);

    for (int i = 0; i < numRead; i++) {
      assertEquals(i + offset, (int) bytes[i]);
    }

    is.close();
  }

  /**
   * Write a binary record to a LobFile. This allows the declared length
   * of the record to disagree with the actual length (the actual length
   * should be &gt;= the declared length).
   * The record written will have values v[i] = i + offset.
   * @param writer the LobFile writer to put the record into
   * @param declaredLen the length value written into the file itself
   * @param actualLen the true number of bytes to write
   * @param offset an amount to adjust each record's byte values by.
   */
  private void writeBlobRecord(LobFile.Writer writer, long declaredLen,
      long actualLen, int offset) throws Exception {
    OutputStream os = writer.writeBlobRecord(declaredLen);
    for (int i = 0; i < actualLen; i++) {
      os.write(i + offset);
    }

    os.close();
    writer.finishRecord();
  }

  /**
   * Verifies a number of records that all have the same declared
   * and actual record lengths.
   * @param p the path to the LobFile to open
   * @param numRecords the number of records to expect
   * @param declaredLen the declared length of each record in the file
   * @param actualLen the true number of bytes we expect to read per record.
   */
  private void verifyBlobRecords(Path p, int numRecords,
      long declaredLen, long actualLen) throws Exception {

    LobFile.Reader reader = LobFile.open(p, conf);
    for (int i = 0; i < numRecords; i++) {
      verifyBlobRecord(reader, declaredLen, actualLen, i);
    }
    assertFalse(reader.next());
    reader.close();
  }

  public void testBinaryRecords() throws Exception {
    // Write a BLOB file and read it all back.

    final long RECORD_LEN = 32;
    final int NUM_RECORDS = 2;
    Path p = new Path(TEMP_BASE_DIR, "binary.lob");
    LobFile.Writer writer = LobFile.create(p, conf);

    for (int i = 0; i < NUM_RECORDS; i++) {
      writeBlobRecord(writer, RECORD_LEN, RECORD_LEN, i);
    }

    writer.close();

    // Now check the read-back on those records.
    verifyBlobRecords(p, NUM_RECORDS, RECORD_LEN, RECORD_LEN);
  }

  public void testOverLengthBinaryRecord() throws Exception {
    // Write a record with a declared length shorter than the
    // actual length, and read it back.

    final long ACTUAL_RECORD_LEN = 48;
    final long DECLARED_RECORD_LEN = 32;
    final int NUM_RECORDS = 2;

    Path p = new Path(TEMP_BASE_DIR, "overlength.lob");
    LobFile.Writer writer = LobFile.create(p, conf);

    for (int i = 0; i < NUM_RECORDS; i++) {
      writeBlobRecord(writer, DECLARED_RECORD_LEN, ACTUAL_RECORD_LEN, i);
    }

    writer.close();

    // Now read them back.
    verifyBlobRecords(p, NUM_RECORDS, DECLARED_RECORD_LEN, ACTUAL_RECORD_LEN);
  }

  private void runCompressedTest(String codec) throws Exception {
    LOG.info("Testing with codec: " + codec);
    Path p = new Path(TEMP_BASE_DIR, "compressed-" + codec + ".lob");
    String [] records = {
      "this is the first record, It should be compressed a lot!",
      "record 2 record 2 record 2 record 2 2 2 2 2 2 2 2 2 2 2 2",
      "and a third and a third yes this is the third",
    };

    runClobFileTest(p, codec, records);
  }

  public void testCompressedFile() throws Exception {
    // Test all the various compression codecs.

    // The following values for 'codec' should pass.
    runCompressedTest(null);
    runCompressedTest(CodecMap.NONE);
    runCompressedTest(CodecMap.DEFLATE);

    try {
      // We expect this to throw UnsupportedCodecException
      // because this class is not included in our package.
      runCompressedTest(CodecMap.LZO);
      fail("Expected unsupported codec exception for lzo");
    } catch (UnsupportedCodecException uce) {
      // We pass.
      LOG.info("Got unsupported codec exception for lzo; expected -- good.");
    }
  }
}

