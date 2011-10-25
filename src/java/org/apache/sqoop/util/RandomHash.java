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

package org.apache.sqoop.util;

import java.rmi.server.UID;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Securely generate random MD5 signatures for use as nonce values.
 */
public final class RandomHash {

  private RandomHash() { }

  /**
   * Generate a new random md5 hash.
   * @return a securely-generated random 16 byte sequence.
   */
  public static byte [] generateMD5Bytes() {
    try {
      MessageDigest digester = MessageDigest.getInstance("MD5");
      long time = System.currentTimeMillis();
      digester.update((new UID() + "@" + time).getBytes());
      return digester.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generate a new random md5 hash and convert it to a string.
   * @return a securely-generated random string.
   */
  public static String generateMD5String() {
    byte [] bytes = generateMD5Bytes();
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      int x = ((int) b) & 0xFF;
      sb.append(String.format("%02x", x));
    }
    return sb.toString();
  }
}

