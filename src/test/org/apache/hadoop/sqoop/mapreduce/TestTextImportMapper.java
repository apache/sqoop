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

package org.apache.hadoop.sqoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.sqoop.lib.LargeObjectLoader;
import org.apache.hadoop.sqoop.lib.SqoopRecord;

import junit.framework.TestCase;

/**
 * Test the TextImportMapper
 */
public class TestTextImportMapper extends TestCase {


  static class DummySqoopRecord implements SqoopRecord {
    long field;

    public DummySqoopRecord(final long val) {
      this.field = val;
    }

    public void readFields(DataInput in) throws IOException {
      field = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(field);
    }

    public void readFields(ResultSet rs) throws SQLException {
      field = rs.getLong(1);
    }

    public void write(PreparedStatement s) throws SQLException {
      s.setLong(1, field);
    }

    public String toString() {
      return "" + field;
    }

    public void loadLargeObjects(LargeObjectLoader loader) { }
    public void parse(CharSequence s) { }
    public void parse(Text s) { }
    public void parse(byte [] s) { }
    public void parse(char [] s) { }
    public void parse(ByteBuffer s) { }
    public void parse(CharBuffer s) { }
  }

  public void testTextImport() {
    TextImportMapper m = new TextImportMapper();
    MapDriver<LongWritable, SqoopRecord, Text, NullWritable> driver =
      new MapDriver<LongWritable, SqoopRecord, Text, NullWritable>(m);

    driver.withInput(new LongWritable(0), new DummySqoopRecord(42))
          .withOutput(new Text("42"), NullWritable.get())
          .runTest();
  }
}
