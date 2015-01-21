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
package org.apache.sqoop.utils;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * Miscellaneous utility methods that help in URL-safe communication over HTTP.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class UrlSafeUtils {

  public static final String ENCODING_UTF8 = "UTF-8";

  public static String urlEncode(String string) {
    try {
      return URLEncoder.encode(string, ENCODING_UTF8);
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException(uee);
    }
  }

  public static String urlDecode(String string) {
    try {
      return URLDecoder.decode(string, ENCODING_UTF8);
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException(uee);
    }
  }


  private UrlSafeUtils() {
    // Disable explicit object creation
  }
}
