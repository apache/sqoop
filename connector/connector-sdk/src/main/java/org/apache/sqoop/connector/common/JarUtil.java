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
package org.apache.sqoop.connector.common;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The helper class provides methods for loading jars.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class JarUtil {

  /**
   * Returns a list of matched jars from current thread or an empty list.
   */
  public static List<String> getMatchedJars(Pattern[] patterns) {
    List<String> jars = new LinkedList<String>();
    for (URL url : ((URLClassLoader) Thread.currentThread()
        .getContextClassLoader()).getURLs()) {
      if (isDesiredJar(url.getPath(), patterns) &&
          !jars.contains(url.getFile())) {
        jars.add(url.toString());
      }
    }
    return jars;
  }

  private static boolean isDesiredJar(String path, Pattern[] patterns) {
    if (path.endsWith(".jar")) {
      for (Pattern pattern : patterns) {
        if (pattern.matcher(path).find()) {
          return true;
        }
      }
    }
    return false;
  }

}