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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.sqoop.connector.idf.CSVIntermediateDataFormat;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Text;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestSqoopWritable {

  private SqoopWritable writable;
  private IntermediateDataFormat<?> idfMock;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    idfMock = mock(IntermediateDataFormat.class);
    writable = new SqoopWritable(idfMock);
  }

  @Test
  public void testWrite() throws IOException {
    String testData = "One ring to rule them all";
    ByteArrayOutputStream ostream = new ByteArrayOutputStream();
    ostream.write(testData.getBytes());
    DataOutput out = new DataOutputStream(ostream);
    writable.write(out);
    // verify that the idf method is called, that is all the test should test
    verify(idfMock, times(1)).write(out);
    ostream.close();
  }

  @Test
  public void testReadFields() throws IOException {
    String testData = "Brandywine Bridge - 20 miles!";
    InputStream instream = new ByteArrayInputStream(testData.getBytes());
    DataInput in = new DataInputStream(instream);
    writable.readFields(in);
    // verify that the idf method is called, that is all the test should test
    verify(idfMock, times(1)).read(in);
    instream.close();
  }

  // NOTE: This test is testing that the write and readFields methods work
  // and not really testing anything about SqoopWritable. Have kept this test since
  // it existed before.
  @Test
  public void testWriteAndReadFields() throws IOException {
    Schema schema = new Schema("test").addColumn(new Text("t"));
    String testData = "You shall not pass";
    ByteArrayOutputStream ostream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(ostream);
    SqoopWritable writableOne = new SqoopWritable(new CSVIntermediateDataFormat(schema));
    writableOne.setString(testData);
    writableOne.write(out);
    byte[] written = ostream.toByteArray();

    // Don't test what the data is, test that SqoopWritable can read it.
    InputStream instream = new ByteArrayInputStream(written);
    SqoopWritable writableTwo = new SqoopWritable(new CSVIntermediateDataFormat(schema));
    DataInput in = new DataInputStream(instream);
    writableTwo.readFields(in);
    assertEquals(writableOne.toString(), writableTwo.toString());
    ostream.close();
    instream.close();
  }

}
