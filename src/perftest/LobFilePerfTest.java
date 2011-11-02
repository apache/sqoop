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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import com.cloudera.sqoop.io.*;

/**
 * A simple benchmark to performance test LobFile reader/writer speed.
 * Writes out 10 GB of data to the local disk and then reads it back.
 * Run with:
 * HADOOP_OPTS=-agentlib:hprof=cpu=samples \
 *     src/scripts/run-perftest.sh LobFilePerfTest
 */
public class LobFilePerfTest {

  private long recordLen = 20 * 1024 * 1024; // 20 MB records
  private int numRecords = 500;
  private Configuration conf;
  private Path p;
  private long startTime;
  private byte [] record;

  public LobFilePerfTest() {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    p = new Path("foo.lob");
  }


  private void startTiming(String s) {
    System.out.println(s);
    startTime = System.currentTimeMillis();
  }

  private void stopTiming() {
    long finishTime = System.currentTimeMillis();
    long delta = finishTime - startTime;
    System.out.println("Finished. Time elapsed: " + delta);
  }

  private void makeRecordBody() {
    startTiming("Allocating record");
    record = new byte[(int)recordLen];
    for (long i = 0; i < recordLen; i++) {
      record[(int)i] = (byte) (i % 256);
    }
    stopTiming();
  }

  private void writeFile() throws Exception {
    startTiming("Writing " + numRecords + " records to lob file");
    LobFile.Writer w = LobFile.create(p, conf);
    for (int i = 0; i < numRecords; i++) {
      OutputStream out = w.writeBlobRecord(recordLen);
      out.write(record);
      out.close();
      w.finishRecord();
    }
    w.close();
    stopTiming();
  }

  private void readFile() throws Exception {
    startTiming("Reading from lob file");
    LobFile.Reader r = LobFile.open(p, conf);
    int receivedRecs = 0;
    byte [] inputBuffer = new byte[4096];
    long recordSize = 0;
    while (r.next()) {
      receivedRecs++;
      InputStream in = r.readBlobRecord();
      while (true) {
        int thisRead = in.read(inputBuffer);
        if (-1 == thisRead) {
          break;
        }
        recordSize += (long) thisRead;
      }
    }
    r.close();
    stopTiming();
    System.out.println("Got " + receivedRecs + " records");
    System.out.println("Read " + recordSize + " bytes");
  }

  public void run() throws Exception {
    makeRecordBody();
    writeFile();
    readFile();
  }

  public static void main(String [] args) throws Exception {
    LobFilePerfTest test = new LobFilePerfTest();
    test.run();
  }
}
