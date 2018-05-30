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

package org.apache.sqoop.mapreduce.mainframe;

import org.apache.hadoop.io.BytesWritable;
import org.apache.sqoop.lib.SqoopRecord;

/**
 * Mapper that writes mainframe dataset records in binary format to multiple files
 * based on the key, which is the index of the datasets in the input split.
 */
public class MainframeDatasetBinaryImportMapper extends AbstractMainframeDatasetImportMapper<BytesWritable> {

  @Override
  protected BytesWritable createOutKey(SqoopRecord sqoopRecord) {
    BytesWritable result = new BytesWritable();
    byte[] bytes = (byte[]) sqoopRecord.getFieldMap().entrySet().iterator().next().getValue();
    result.set(bytes,0, bytes.length);
    return result;
  }
}