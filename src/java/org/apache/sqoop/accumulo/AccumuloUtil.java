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
package org.apache.sqoop.accumulo;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.config.ConfigurationHelper;

/**
 * This class provides a method that checks if Accumulo jars are present in the
 * current classpath. It also provides a setAlwaysNoAccumuloJarMode mechanism
 * for testing and simulation the condition where the is on Accumulo jar (since
 * Accumulo is pulled automatically by ivy)
 */
public final class AccumuloUtil {
  private static final Log LOG = LogFactory.getLog(AccumuloUtil.class);
  private static boolean testingMode = false;

  private static final String INSTANCE_CLASS
    = "org.apache.accumulo.core.client.Instance";

  // Prevent instantiation
  private AccumuloUtil() {
  }

  /**
   * This is a way to make this always return false for testing.
   */
  public static void setAlwaysNoAccumuloJarMode(boolean mode) {
    testingMode = mode;
  }

  public static boolean isAccumuloJarPresent() {
    if (testingMode) {
      return false;
    }
    try {
      Class.forName(INSTANCE_CLASS);
    } catch (ClassNotFoundException cnfe) {
      return false;
    }
    return true;
  }

  /**
   * Add the Accumulo jar files to local classpath and dist cache.
   * @throws IOException
   */
  public static void addJars(Job job, SqoopOptions options) throws IOException {

    if (ConfigurationHelper.isLocalJobTracker(job.getConfiguration())) {
      LOG.info("Not adding Accumulo jars to distributed cache in local mode");
    } else if (options.isSkipDistCache()) {
      LOG.info("Not adding Accumulo jars to distributed cache as requested");
    } else {
      Configuration conf = job.getConfiguration();
      String accumuloHome = null;
      String zookeeperHome = null;
      FileSystem fs = FileSystem.getLocal(conf);
      if (options != null) {
        accumuloHome = options.getAccumuloHome();
      }
      if (accumuloHome == null) {
        accumuloHome = SqoopOptions.getAccumuloHomeDefault();
      }
      LOG.info("Accumulo job : Accumulo Home = " + accumuloHome);
      if (options != null) {
        zookeeperHome = options.getZookeeperHome();
      }
      if (zookeeperHome == null) {
        zookeeperHome = SqoopOptions.getZookeeperHomeDefault();
      }
      LOG.info("Accumulo job : Zookeeper Home = " + zookeeperHome);

      conf.addResource(accumuloHome + AccumuloConstants.ACCUMULO_SITE_XML_PATH);

      // Add any libjars already specified
      Set<String> localUrls = new HashSet<String>();
      localUrls
        .addAll(conf.getStringCollection(
          ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM));

      if (null == accumuloHome) {
        throw new IllegalArgumentException("ACCUMULO_HOME is not set.");
      } else {
        File dir = new File(accumuloHome, "lib");
        String path = dir.getPath();
        LOG.info("Adding jar files under " + path + " to distributed cache");
        addDirToCache(dir, fs, localUrls, false);
      }

      if (null == zookeeperHome) {
        throw new IllegalArgumentException("ZOOKEEPER_HOME is not set.");
      } else {
        String dir = zookeeperHome;
        LOG.info("Adding jar files under " + dir + " to distributed cache");
        addDirToCache(new File(dir), fs, localUrls, false);
      }

      String tmpjars = conf
        .get(ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM);
      StringBuilder sb = new StringBuilder(1024);
      if (null != tmpjars) {
        sb.append(tmpjars);
        sb.append(",");
      }
      sb.append(StringUtils.arrayToString(localUrls.toArray(new String[0])));
      conf.set(ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM,
        sb.toString());
    }
  }

  /**
   * Add the .jar elements of a directory to the DCache classpath, optionally
   * recursively.
   */
  private static void addDirToCache(File dir, FileSystem fs,
    Set<String> localUrls, boolean recursive) {
    if (dir != null) {
      File[] fileList = dir.listFiles();

      if (fileList != null) {
        for (File libFile : dir.listFiles()) {
          if (libFile.exists() && !libFile.isDirectory()
            && libFile.getName().endsWith("jar")) {
            Path p = new Path(libFile.toString());
            if (libFile.canRead()) {
              String qualified = p.makeQualified(fs).toString();
              LOG.info("Adding to job classpath: " + qualified);
              localUrls.add(qualified);
            } else {
              LOG.warn("Ignoring unreadable file " + libFile);
            }
          }
          if (recursive && libFile.isDirectory()) {
            addDirToCache(libFile, fs, localUrls, recursive);
          }
        }
      } else {
        LOG.warn("No files under " + dir
          + " to add to distributed cache for Accumulo job");
      }
    }
  }

}
