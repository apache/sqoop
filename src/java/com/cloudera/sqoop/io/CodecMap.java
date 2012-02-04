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

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * Provides a mapping from codec names to concrete implementation class names.
 *
 * @deprecated use org.apache.sqoop.io.CodecMap instead.
 * @see org.apache.sqoop.io.CodecMap
 */
public final class CodecMap {

  // Supported codec map values
  // Note: do not add more values here, since codecs are discovered using the
  // standard Hadoop mechanism (io.compression.codecs). See
  // CompressionCodecFactory.
  public static final String NONE = org.apache.sqoop.io.CodecMap.NONE;
  public static final String DEFLATE = org.apache.sqoop.io.CodecMap.DEFLATE;
  public static final String LZO = org.apache.sqoop.io.CodecMap.LZO;
  public static final String LZOP = org.apache.sqoop.io.CodecMap.LZOP;

  private CodecMap() {
  }

  /**
   * Given a codec name, return the name of the concrete class
   * that implements it (or 'null' in the case of the "none" codec).
   * @throws UnsupportedCodecException if a codec cannot be found
   * with the supplied name.
   */
  public static String getCodecClassName(String codecName)
      throws UnsupportedCodecException {
    return org.apache.sqoop.io.CodecMap.getCodecClassName(codecName);
  }

  /**
   * Given a codec name, instantiate the concrete implementation
   * class that implements it.
   * @throws UnsupportedCodecException if a codec cannot be found
   * with the supplied name.
   */
  public static CompressionCodec getCodec(String codecName,
      Configuration conf) throws UnsupportedCodecException {
    return org.apache.sqoop.io.CodecMap.getCodec(codecName, conf);
  }

  /**
   * Return the set of available codec names.
   */
  public static Set<String> getCodecNames() {
    return org.apache.sqoop.io.CodecMap.getCodecNames();
  }

  /**
   * Return the short name of the codec.
   * See {@link org.apache.sqoop.io.CodecMap#getCodecShortNameByName(String,
   * Configuration)}.
   */
  public static String getCodecShortNameByName(String codecName,
    Configuration conf) throws UnsupportedCodecException {
    return org.apache.sqoop.io.CodecMap
      .getCodecShortNameByName(codecName, conf);
  }
}
