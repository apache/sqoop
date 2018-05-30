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

package org.apache.sqoop.mapreduce;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

public class KeyRecordWriters<K,V> {
  /**
   * RecordWriter to write to plain text files.
   */

  public static class GenericRecordWriter<K, V> extends RecordWriter<K, V> {
    protected static final String UTF8 = "UTF-8";

    protected DataOutputStream out;

    /**
     * Write the object to the byte stream, handling Text as a special
     * case.
     *
     * @param o the object to print
     * @param value the corresponding value for key o
     * @throws IOException if the write throws, we pass it on
     */
    protected void writeObject(Object o,Object value) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        out.write(o.toString().getBytes(UTF8));
      }
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      writeObject(key,value);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        out.close();
    }
  }

  public static class RawKeyRecordWriter<K, V> extends GenericRecordWriter<K, V> {

    public RawKeyRecordWriter(DataOutputStream out) {
      this.out = out;
    }
  }

  /**
   * RecordWriter to write to plain text files.
   */
  public static class BinaryKeyRecordWriter<K, V> extends GenericRecordWriter<K, V> {

    public BinaryKeyRecordWriter(DataOutputStream out) {
      this.out = out;
    }

    /**
     * Write the object to the byte stream, handling Text as a special
     * case.
     * @param o the object to print
     * @throws IOException if the write throws, we pass it on
     */
    @Override
    protected void writeObject(Object o, Object value) throws IOException {
      if (o instanceof BytesWritable) {
        BytesWritable to = (BytesWritable) o;
        out.write(to.getBytes(), 0, to.getLength());
      }
    }
  }
}
