/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.job.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;

public class TestSqoopWritable {

  private final SqoopWritable writable = new SqoopWritable();

  @Test
  public void testStringInStringOut() {
    String testData = "Live Long and prosper";
    writable.setString(testData);
    Assert.assertEquals(testData,writable.getString());
  }

  @Test
  public void testDataWritten() throws IOException {
    String testData = "One ring to rule them all";
    writable.setString(testData);
    ByteArrayOutputStream ostream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(ostream);
    writable.write(out);
    byte[] written = ostream.toByteArray();
    InputStream instream = new ByteArrayInputStream(written);
    DataInput in = new DataInputStream(instream);
    String readData = in.readUTF();
    Assert.assertEquals(testData, readData);
  }

  @Test
  public void testDataRead() throws IOException {
    String testData = "Brandywine Bridge - 20 miles!";
    ByteArrayOutputStream ostream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(ostream);
    out.writeUTF(testData);
    InputStream instream = new ByteArrayInputStream(ostream.toByteArray());
    DataInput in = new DataInputStream(instream);
    writable.readFields(in);
    Assert.assertEquals(testData, writable.getString());
  }

  @Test
  public void testWriteReadUsingStream() throws IOException {
    String testData = "You shall not pass";
    ByteArrayOutputStream ostream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(ostream);
    writable.setString(testData);
    writable.write(out);
    byte[] written = ostream.toByteArray();

    //Don't test what the data is, test that SqoopWritable can read it.
    InputStream instream = new ByteArrayInputStream(written);
    SqoopWritable newWritable = new SqoopWritable();
    DataInput in = new DataInputStream(instream);
    newWritable.readFields(in);
    Assert.assertEquals(testData, newWritable.getString());
    ostream.close();
    instream.close();
  }

}
