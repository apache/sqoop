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

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import com.cloudera.sqoop.io.*;

/**
 * Stress test LobFiles by writing a bunch of different files and reading
 * them back in various ways.
 * Run with: src/scripts/run-perftest.sh LobFileStressTest
 */
public class LobFileStressTest {

  // Big records in testBigFile() are 5 GB each.
  public static final long LARGE_RECORD_LEN = 5L * 1024L * 1024L * 1024L;

  private int numRandomTrials = 1000000;
  private Configuration conf;
  private boolean allPassed;

  private long lastCompressPos; // start offset of the last record in the file.
  private long lastRawPos;

  public LobFileStressTest() {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    allPassed = true;
  }

  private Path getPath(boolean compress) {
    if (compress) {
      return new Path("compressed.lob");
    } else {
      return new Path("integers.lob");
    }
  }

  private long getLastRecordPos(boolean compress) {
    if (compress) {
      return lastCompressPos;
    } else {
      return lastRawPos;
    }
  }

  private void setLastRecordPos(long pos, boolean compress) {
    if (compress) {
      lastCompressPos = pos;
    } else {
      lastRawPos = pos;
    }
  }

  private int getNumRecords(boolean compress) {
    if (!compress) {
      return 40000000; // 40 million.
    } else {
      return 5000000; // 5 million; the compressor is just too slow for 40M.
    }
  }

  private void writeIntegerFile(boolean compress) throws Exception {
    boolean passed = false;
    try {
      System.out.print("Writing integers file. compress=" + compress + ". ");
      Path p = getPath(compress);
      FileSystem fs = FileSystem.getLocal(conf);
      if (fs.exists(p)) {
        fs.delete(p, false);
      }
      String codecName = compress ? "deflate" : null;
      LobFile.Writer w = LobFile.create(p, conf, false, codecName);

      int numRecords = getNumRecords(compress);
      for (int i = 0; i < numRecords; i++) {
        setLastRecordPos(w.tell(), compress);
        OutputStream os = w.writeBlobRecord(0);
        DataOutputStream dos = new DataOutputStream(os);
        dos.writeInt(i);
        dos.close();
        os.close();
      }

      w.close();
      System.out.println("PASS");
      passed = true;
    } finally {
      if (!passed) {
        allPassed = false;
        System.out.println("FAIL");
      }
    }
  }

  private void testSequentialScan(boolean compress) throws Exception {
    // Write a LobFile containing several million integers
    // and read the entire resulting file, verify that they
    // all appear in order.
    boolean success = false;
    System.out.print("Testing sequential scan. compress=" + compress + ". ");
    try {
      LobFile.Reader r = LobFile.open(getPath(compress), conf);
      int numRecords = getNumRecords(compress);
      for (int i = 0; i < numRecords; i++) {
        r.next();
        if (!r.isRecordAvailable()) {
          throw new Exception("File ended early; i=" + i);
        }
        long id = r.getRecordId();
        if (id != i) {
          throw new Exception("record id mismatch; expected " + i
              + " got " + id);
        }

        InputStream is = r.readBlobRecord();
        DataInputStream dis = new DataInputStream(is);
        int val = dis.readInt();
        if (val != i) {
          throw new Exception("Unexpected value in stream; expected " + i
              + " got " + val);
        }

        try {
          // Make sure we can't read additional data from the stream.
          byte b = dis.readByte();
          throw new Exception("Got unexpected extra byte : " + b);
        } catch (EOFException eof) {
          // Expected this. Ignore.
        }

        dis.close();
      }
      if (r.next()) {
        // We should have finished reading everything already.
        throw new Exception("Additional record was waiting at end of file");
      }
      r.close();
      System.out.println("PASS");
      success = true;
    } finally {
      if (!success) {
        allPassed = false;
        System.out.println("FAIL");
      }
    }
  }

  private void testSmallSeeks(boolean compress) throws Exception {
    // Write a LobFile containing several million integers
    // and seek to randomly selected records. Make sure we can
    // access each one.
    boolean success = false;
    long seed = System.currentTimeMillis();
    long lastRecordPos = getLastRecordPos(compress);
    System.out.print("Testing random seeks. compress=" + compress + ". "
        + ". seed=" + seed + ". lastPos=" + lastRecordPos +". ");
    try {
      LobFile.Reader r = LobFile.open(getPath(compress), conf);
      Random rnd = new Random(seed);
      long curRecord = -1; // current position starts before record 0.
      int numRecords = getNumRecords(compress);
      for (int i = 0; i < numRandomTrials; i++) {
        if (rnd.nextInt(100) < 20 && curRecord != numRecords - 1) {
          // Sequentially access the next record.
          if (!r.next()) {
            throw new Exception("cur record is " + curRecord
                + " but sequential next() failed!");
          }

          curRecord++;
        } else {
          // Randomly find a record.
          long targetPos = rnd.nextLong() % (lastRecordPos + 1);
          r.seek(targetPos);
          if (!r.next()) {
            throw new Exception("Could not seek to record starting at "
                + targetPos);
          }

          curRecord = r.getRecordId();
          if (curRecord < 0 || curRecord >= numRecords) {
            throw new Exception("Unexpected record id " + curRecord);
          }
        }

        // In either case, read the record and verify the data's correct.
        InputStream is = r.readBlobRecord();
        DataInputStream dis = new DataInputStream(is);

        int val = dis.readInt();
        if (val != curRecord) {
          throw new Exception("Unexpected value in stream; expected "
              + curRecord + " got " + val);
        }

        try {
          // Make sure we can't read additional data from the stream.
          byte b = dis.readByte();
          throw new Exception("Got unexpected extra byte : " + b);
        } catch (EOFException eof) {
          // Expected this. Ignore.
        }

        dis.close();
      }

      r.close();
      System.out.println("PASS");
      success = true;
    } finally {
      if (!success) {
        allPassed = false;
        System.out.println("FAIL");
      }
    }
  }

  private Path getBigFilePath(boolean compress) {
    if (compress) {
      return new Path("big-compressed.lob");
    } else {
      return new Path("big.lob");
    }
  }

  /**
   * Check that the bytes in buf are of the form 0 1 2 .. 254 255 0 1 2..
   * but offset by the value in 'firstByte'. Returns the next byte value
   * we would expect in an adjacent follow-up array of bytes.
   */
  private byte checkBuf(byte [] buf, int len, byte firstByte) throws Exception {
    byte b = firstByte;
    for (int i = 0; i < len; i++) {
      if (buf[i] != b++) {
        throw new Exception("Expected value " + b + " but got " + buf[i]);
      }
    }

    return b;
  }

  /**
   * Check that a big record (from testBigFile) matches its expected
   * contract.
   * The reader should be positioned at the beginning of the record
   * with id 'expectedId'.
   */
  private void checkBigRecord(LobFile.Reader r, long expectedId)
      throws Exception {
    if (!r.isRecordAvailable()) {
      throw new Exception("No record available");
    }

    if (r.getRecordId() != expectedId) {
      throw new Exception("Expected record id=" + expectedId
          + "; got " + r.getRecordId());
    }

    // Read all the data in this record, check that it is the data
    // we expect. The bytes in each record increase 0 1 2 .. 254 255 0 1 2..
    // but each record's start byte is offset by its record id.
    long received = 0;
    InputStream is = r.readBlobRecord();
    long expected = LARGE_RECORD_LEN;
    final int BUF_SIZE = 16384;
    byte [] buf = new byte[BUF_SIZE];
    byte last = (byte) r.getRecordId();
    while (expected > 0) {
      int thisRead = is.read(buf);
      if (thisRead == -1) {
        break;
      }

      last = checkBuf(buf, thisRead, last);
      expected -= thisRead;
    }

    if (expected > 0) {
      throw new Exception("Couldn't read all the data! expected "
          + expected + " more bytes");
    }

    if (is.read() != -1) {
      throw new Exception("Got an extra byte! Expected no more data.");
    }
  }

  private void testBigFile(boolean compress) throws Exception {
    // Write a file containing 5 GB records.

    final int NUM_RECORDS = 5;
    boolean passed = false;

    try {
      System.out.print("Testing large file operations. compress="
          + compress + ". ");

      Path p = getBigFilePath(compress);
      long [] startOffsets = new long[NUM_RECORDS];

      // Write the file. Five records, 5 GB a piece.
      System.out.print("Testing write. ");
      FileSystem fs = FileSystem.getLocal(conf);
      if (fs.exists(p)) {
        fs.delete(p, false);
      }
      String codecName = compress ? "deflate" : null;
      System.out.println("record size: " + LARGE_RECORD_LEN);
      LobFile.Writer w = LobFile.create(p, conf, false, codecName);
      for (int i = 0; i < NUM_RECORDS; i++) {
        startOffsets[i] = w.tell();
        System.out.println("Starting record " + i + " at " + startOffsets[i]);
        OutputStream os = w.writeBlobRecord(0);
        for (long v = 0; v < LARGE_RECORD_LEN; v++) {
          long byteVal = (((long) i) + v) & 0xFF;
          os.write((int) byteVal);
        }
        os.close();
      }
      w.close();
      System.out.println("PASS");

      // Iterate past three records, read the fourth.
      System.out.print("Testing iterated skipping. ");
      LobFile.Reader r = LobFile.open(p, conf);
      for (int i = 0; i < 4; i++) {
        r.next();
      }

      checkBigRecord(r, 3);
      System.out.println("PASS");

      // Seek directly to record 2, read it through.
      System.out.print("Testing large backward seek. ");
      r.seek(startOffsets[2]);
      r.next();
      checkBigRecord(r, 2);
      System.out.println("PASS");

      passed = true;
    } finally {
      if (!passed) {
        allPassed = false;
        System.out.println("FAIL");
      }
    }
  }

  public void run() throws Exception {
    writeIntegerFile(true);
    writeIntegerFile(false);
    testSequentialScan(false);
    testSmallSeeks(false);
    testSequentialScan(true);
    testSmallSeeks(true);
    testBigFile(false);
    testBigFile(true);

    if (allPassed) {
      System.out.println("Tests passed.");
    } else {
      System.out.println("All tests did not pass!");
      System.exit(1);
    }

  }

  public static void main(String [] args) throws Exception {
    LobFileStressTest test = new LobFileStressTest();
    test.run();
  }
}
