/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Provides a mapping from codec names to concrete implementation class names.
 * This is used by LobFile.
 */
public final class CodecMap {

  // Supported codec map values
  public static final String NONE = "none";
  public static final String DEFLATE = "deflate";
  public static final String LZO = "lzo";

  private static Map<String, String> codecNames;
  static {
    codecNames = new TreeMap<String, String>();

    // Register the names of codecs we know about.
    codecNames.put(NONE,    null);
    codecNames.put(DEFLATE, "org.apache.hadoop.io.compress.DefaultCodec");
    codecNames.put(LZO,     "com.hadoop.compression.lzo.LzoCodec");
  }

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
    if (!codecNames.containsKey(codecName)) {
      throw new UnsupportedCodecException(codecName);
    }

    return codecNames.get(codecName);
  }

  /**
   * Given a codec name, instantiate the concrete implementation
   * class that implements it.
   * @throws UnsupportedCodecException if a codec cannot be found
   * with the supplied name.
   */
  public static CompressionCodec getCodec(String codecName,
      Configuration conf) throws UnsupportedCodecException {
    String codecClassName = null;
    try {
      codecClassName = getCodecClassName(codecName);
      if (null == codecClassName) {
        return null;
      }
      Class<? extends CompressionCodec> codecClass =
          (Class<? extends CompressionCodec>)
          conf.getClassByName(codecClassName);
      return (CompressionCodec) ReflectionUtils.newInstance(
          codecClass, conf);
    } catch (ClassNotFoundException cnfe) {
      throw new UnsupportedCodecException("Cannot find codec class "
          + codecClassName + " for codec " + codecName);
    }
  }

  /**
   * Return the set of available codec names.
   */
  public static Set<String> getCodecNames() {
    return codecNames.keySet();
  }
}
