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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;

import org.junit.rules.ExpectedException;

/**
 * Test looking up codecs by name.
 */
public class TestCodecMap  {


  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private void verifyCodec(Class<?> c, String codecName)
      throws UnsupportedCodecException {
    CompressionCodec codec = CodecMap.getCodec(codecName, new Configuration());
    assertEquals(codec.getClass(), c);
  }

  @Test
  public void testGetCodecNames() {
    // gzip is picked up from Hadoop defaults
    assertTrue(CodecMap.getCodecNames().contains("gzip"));
  }

  @Test
  public void testGetCodec() throws IOException {
    verifyCodec(GzipCodec.class, "gzip");
    verifyCodec(GzipCodec.class, "Gzip");
    verifyCodec(GzipCodec.class, "GZIP");
    verifyCodec(GzipCodec.class, "gzipcodec");
    verifyCodec(GzipCodec.class, "GzipCodec");
    verifyCodec(GzipCodec.class, "GZIPCODEC");
    verifyCodec(GzipCodec.class, "org.apache.hadoop.io.compress.GzipCodec");
  }

  @Test
  public void testGetShortName() throws UnsupportedCodecException {
    verifyShortName("gzip", "org.apache.hadoop.io.compress.GzipCodec");
    verifyShortName("default", "org.apache.hadoop.io.compress.DefaultCodec");

    thrown.expect(UnsupportedCodecException.class);
    thrown.reportMissingExceptionWithMessage("Expected UnsupportedCodecException with invalid codec name during getting " +
        "short codec name");
    verifyShortName("NONE", "bogus");
  }

  private void verifyShortName(String expected, String codecName)
    throws UnsupportedCodecException {
    assertEquals(expected,
      CodecMap.getCodecShortNameByName(codecName, new Configuration()));
  }

  @Test
  public void testUnrecognizedCodec() throws UnsupportedCodecException {
    thrown.expect(UnsupportedCodecException.class);
    thrown.reportMissingExceptionWithMessage("Expected UnsupportedCodecException with invalid codec name");
    CodecMap.getCodec("bogus", new Configuration());
  }

}
