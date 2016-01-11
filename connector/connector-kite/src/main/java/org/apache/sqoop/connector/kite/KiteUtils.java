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

package org.apache.sqoop.connector.kite;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.error.code.KiteConnectorError;

/**
 * Utilities for Kite.
 */
public class KiteUtils {

  private static final Logger LOG = Logger.getLogger(KiteUtils.class);

  private static final String DEFAULT_HADOOP_CONF_DIR = "/etc/hadoop/conf";

  @edu.umd.cs.findbugs.annotations.SuppressWarnings({"SIC_INNER_SHOULD_BE_STATIC_ANON"})
  public static void addConfigDirToClasspath(final LinkConfiguration linkConfig) {
    File configDir = new File(getConfDir(linkConfig));
    try {
      final Method method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[]{URL.class});
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          method.setAccessible(true);
          return null;
        }
      });
      method.invoke(ClassLoader.getSystemClassLoader(), new Object[]{configDir.toURI().toURL()});
    } catch (NoSuchMethodException | SecurityException
        | InvocationTargetException | IllegalAccessException
        | IllegalArgumentException | MalformedURLException e) {
      throw new SqoopException(KiteConnectorError.GENERIC_KITE_CONNECTOR_0004, e);
    }
    LOG.debug("Added file " + configDir + " to classpath");
  }

  private static String getConfDir(LinkConfiguration linkConfig) {
    String confDir = linkConfig.linkConfig.confDir;
    if (StringUtils.isBlank(confDir)) {
      confDir = DEFAULT_HADOOP_CONF_DIR;
    }
    return confDir;
  }
}
