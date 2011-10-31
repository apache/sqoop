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

package com.cloudera.sqoop.io;

/**
 * A BufferedWriter implementation that wraps around a SplittingOutputStream
 * and allows splitting of the underlying stream.
 * Splits occur at allowSplit() calls, or newLine() calls.
 *
 * @deprecated use org.apache.sqoop.io.SplittableBufferedWriter instead.
 * @see org.apache.sqoop.io.SplittableBufferedWriter
 */
public class SplittableBufferedWriter
  extends org.apache.sqoop.io.SplittableBufferedWriter {

  public SplittableBufferedWriter(
      final SplittingOutputStream splitOutputStream) {
    super(splitOutputStream);
  }

  SplittableBufferedWriter(final SplittingOutputStream splitOutputStream,
      final boolean alwaysFlush) {
    super(splitOutputStream, alwaysFlush);
  }
}
