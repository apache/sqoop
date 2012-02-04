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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import junit.framework.TestCase;

/**
 * Test looking up codecs by name.
 */
public class TestCodecMap extends TestCase {

  private void verifyCodec(Class<?> c, String codecName)
      throws UnsupportedCodecException {
    CompressionCodec codec = CodecMap.getCodec(codecName, new Configuration());
    assertEquals(codec.getClass(), c);
  }

  public void testGetCodecNames() {
    // gzip is picked up from Hadoop defaults
    assertTrue(CodecMap.getCodecNames().contains("gzip"));
  }

  public void testGetCodec() throws IOException {
    verifyCodec(GzipCodec.class, "gzip");
    verifyCodec(GzipCodec.class, "Gzip");
    verifyCodec(GzipCodec.class, "GZIP");
    verifyCodec(GzipCodec.class, "gzipcodec");
    verifyCodec(GzipCodec.class, "GzipCodec");
    verifyCodec(GzipCodec.class, "GZIPCODEC");
    verifyCodec(GzipCodec.class, "org.apache.hadoop.io.compress.GzipCodec");
  }

  public void testGetShortName() throws UnsupportedCodecException {
    verifyShortName("gzip", "org.apache.hadoop.io.compress.GzipCodec");
    verifyShortName("default", "org.apache.hadoop.io.compress.DefaultCodec");
    try {
      verifyShortName("NONE", "bogus");
      fail("Expected IOException");
    } catch (UnsupportedCodecException e) {
      // Exception is expected
    }
  }

  private void verifyShortName(String expected, String codecName)
    throws UnsupportedCodecException {
    assertEquals(expected,
      CodecMap.getCodecShortNameByName(codecName, new Configuration()));
  }

  public void testUnrecognizedCodec() {
    try {
      CodecMap.getCodec("bogus", new Configuration());
      fail("'bogus' codec should throw exception");
    } catch (UnsupportedCodecException e) {
      // expected
    }
  }
}
