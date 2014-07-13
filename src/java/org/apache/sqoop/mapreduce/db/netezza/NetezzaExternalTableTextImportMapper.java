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
package org.apache.sqoop.mapreduce.db.netezza;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Netezza import mapper using external tables for text formats.
 */
public class NetezzaExternalTableTextImportMapper
  extends NetezzaExternalTableImportMapper<Text, NullWritable> {

  @Override
  protected void writeRecord(Text text, Context context)
    throws IOException, InterruptedException {
    // May be we should set the output to be String for faster performance
    // There is no real benefit in changing it to Text and then
    // converting it back in our case
    context.write(text, NullWritable.get());
  }
}
