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
package org.apache.hadoop.sqoop.shims;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.VersionInfo;

/**
 * Provides a service locator for the appropriate shim, dynamically chosen
 * based on the Hadoop version in the classpath. 
 */
public abstract class ShimLoader {
  private static HadoopShim hadoopShim;

  public static final Log LOG = LogFactory.getLog(ShimLoader.class.getName());

  /**
   * The names of the classes for shimming Hadoop.
   * This list must be maintained in the same order as HADOOP_SHIM_MATCHES
   */
  private static final List<String> HADOOP_SHIM_CLASSES =
      new ArrayList<String>();

  /**
   * The regular expressions compared against the Hadoop version string
   * when determining which shim class to load.
   */
  private static final List<String> HADOOP_SHIM_MATCHES =
      new ArrayList<String>();

  static {
    // These regular expressions will be evaluated in order until one matches.

    // Check 
    HADOOP_SHIM_MATCHES.add("0.20.2-[cC][dD][hH]3.*");
    HADOOP_SHIM_CLASSES.add("org.apache.hadoop.sqoop.shims.CDH3Shim");

    // Apache 0.22 trunk
    HADOOP_SHIM_MATCHES.add("0.22-.*");
    HADOOP_SHIM_CLASSES.add("org.apache.hadoop.sqoop.shims.Apache22HadoopShim");

    // Apache 0.22 trunk snapshots often compile with "Unknown" version,
    // so we default to guessing Apache in this case.
    HADOOP_SHIM_MATCHES.add("Unknown");
    HADOOP_SHIM_CLASSES.add("org.apache.hadoop.sqoop.shims.Apache22HadoopShim");
  }

  /**
   * Factory method to get an instance of HadoopShim based on the
   * version of Hadoop on the classpath.
   */
  public static synchronized HadoopShim getHadoopShim() {
    if (hadoopShim == null) {
      hadoopShim = loadShim(HADOOP_SHIM_MATCHES, HADOOP_SHIM_CLASSES,
          HadoopShim.class);
    }
    return hadoopShim;
  }

  @SuppressWarnings("unchecked")
  private static <T> T loadShim(List<String> matchExprs,
      List<String> classNames, Class<T> xface) {
    String version = VersionInfo.getVersion();

    LOG.debug("Loading shims for class : " + xface.getName());
    LOG.debug("Hadoop version: " + version);

    for (int i = 0; i < matchExprs.size(); i++) {
      if (version.matches(matchExprs.get(i))) {
        String className = classNames.get(i);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Version matched regular expression: " + matchExprs.get(i));
          LOG.debug("Trying to load class: " + className);
        }
        try {
          Class clazz = Class.forName(className);
          return xface.cast(clazz.newInstance());
        } catch (Exception e) {
          throw new RuntimeException("Could not load shim in class " +
              className, e);
        }
      }
    }

    throw new RuntimeException("Could not find appropriate Hadoop shim for "
        + version);
  }

  private ShimLoader() {
    // prevent instantiation
  }
}
