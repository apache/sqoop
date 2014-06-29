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

package org.apache.sqoop.manager.oracle.util;

/**
 * Generates test data for Oracle UriType columns. Generated Strings can be cast
 * to URITypes with sys.UriFactory.getUri(value). Generated strings may be
 * detect as any of HTTPURIType, DBURIType or XDBURIType.
 *
 * Generated URIs are unlikely to resolve successfully, so should be used for
 * import/export tests only, and not used to reference data.
 */
public class URIGenerator extends OraOopTestDataGenerator<String> {
  private static final int MIN_LENGTH = 15;
  private static final int MAX_LENGTH = 30;

  @Override
  public String next() {
    StringBuilder sb = new StringBuilder();
    switch (rng.nextInt(3)) {
      case 0: // Generate a String that will detect as an HTTPURIType
        sb.append("http://");
        break;
      case 1: // Generate a String that will detect as an DBURIType
        sb.append("/oradb/");
        break;
      case 2: // Generate a String that will detect as an XDBURIType
        break;
      default:
        throw new RuntimeException("Invalid number generated.");
    }

    int length =
        sb.length() + MIN_LENGTH + rng.nextInt(MAX_LENGTH - MIN_LENGTH + 1);
    while (sb.length() < length) {
      sb.append(Character.toChars(rng.nextInt(128)));
    }
    return sb.toString();
  }

}
