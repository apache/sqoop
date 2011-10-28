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
package org.apache.sqoop.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.io.input.ProxyInputStream;

/**
 * Provides an InputStream that can consume a fixed maximum number of bytes
 * from an underlying stream. Closing the FixedLengthInputStream does not
 * close the underlying stream. After reading the maximum number of available
 * bytes this acts as though EOF has been reached.
 */
public class FixedLengthInputStream extends ProxyInputStream {

  private CountingInputStream countingIn;
  private long maxBytes;

  public FixedLengthInputStream(InputStream stream, long maxLen) {
    super(new CountingInputStream(new CloseShieldInputStream(stream)));

    // Save a correctly-typed reference to the underlying stream.
    this.countingIn = (CountingInputStream) this.in;
    this.maxBytes = maxLen;
  }

  /** @return the number of bytes already consumed by the client. */
  private long consumed() {
    return countingIn.getByteCount();
  }

  /**
   * @return number of bytes remaining to be read before the limit
   * is reached.
   */
  private long toLimit() {
    return maxBytes - consumed();
  }

  @Override
  public int available() throws IOException {
    return (int) Math.min(toLimit(), countingIn.available());
  }

  @Override
  public int read() throws IOException {
    if (toLimit() > 0) {
      return super.read();
    } else {
      return -1; // EOF.
    }
  }

  @Override
  public int read(byte [] buf) throws IOException {
    return read(buf, 0, buf.length);
  }

  @Override
  public int read(byte [] buf, int start, int count) throws IOException {
    long limit = toLimit();
    if (limit == 0) {
      return -1; // EOF.
    } else {
      return super.read(buf, start, (int) Math.min(count, limit));
    }
  }
}
