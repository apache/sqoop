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
package com.cloudera.sqoop.lib;

import org.apache.commons.logging.Log;

/**
 * BlobRef is a wrapper that holds a BLOB either directly, or a
 * reference to a file that holds the BLOB data.
 *
 * @deprecated use org.apache.sqoop.lib.BlobRef instead.
 * @see org.apache.sqoop.lib.BlobRef
 */
public class BlobRef extends org.apache.sqoop.lib.BlobRef {

  public static final Log LOG = org.apache.sqoop.lib.BlobRef.LOG;

  public BlobRef() {
    super();
  }

  public BlobRef(byte [] bytes) {
    super(bytes);
  }

  /**
   * Initialize a BlobRef to an external BLOB.
   * @param file the filename to the BLOB. May be relative to the job dir.
   * @param offset the offset (in bytes) into the LobFile for this record.
   * @param length the length of the record in bytes.
   */
  public BlobRef(String file, long offset, long length) {
    super(file, offset, length);
  }


  /**
   * Create a BlobRef based on parsed data from a line of text.
   * This only operates correctly on external blobs; inline blobs are simply
   * returned as null. You should store BLOB data in SequenceFile format
   * if reparsing is necessary.
   * @param inputString the text-based input data to parse.
   * @return a new BlobRef containing a reference to an external BLOB, or
   * an empty BlobRef if the data to be parsed is actually inline.
   */
  public static BlobRef parse(String inputString) {
    return org.apache.sqoop.lib.BlobRef.parse(inputString);
  }
}

