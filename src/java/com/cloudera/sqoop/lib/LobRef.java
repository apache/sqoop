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

import java.util.regex.Matcher;

import org.apache.commons.logging.Log;

/**
 * Abstract base class that holds a reference to a Blob or a Clob.
 * DATATYPE is the type being held (e.g., a byte array).
 * CONTAINERTYPE is the type used to hold this data (e.g., BytesWritable).
 * ACCESSORTYPE is the type used to access this data in a streaming fashion
 *   (either an InputStream or a Reader).
 *
 * @deprecated use org.apache.sqoop.lib.LobRef instead.
 * @see org.apache.sqoop.lib.LobRef
 */
public abstract class LobRef<DATATYPE, CONTAINERTYPE, ACCESSORTYPE>
    extends org.apache.sqoop.lib.LobRef<DATATYPE, CONTAINERTYPE, ACCESSORTYPE> {

  public static final Log LOG = org.apache.sqoop.lib.LobRef.LOG;

  protected LobRef() {
    super();
  }

  protected LobRef(CONTAINERTYPE container) {
    super(container);
  }

  protected LobRef(String file, long offset, long length) {
    super(file, offset, length);
  }

  protected static final ThreadLocal<Matcher> EXTERNAL_MATCHER =
    org.apache.sqoop.lib.LobRef.EXTERNAL_MATCHER;
}

