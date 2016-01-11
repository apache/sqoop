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
package org.apache.sqoop.test.minicluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.test.kdc.KdcRunner;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 */
public class SqoopMiniClusterFactory {

  public static final String MINICLUSTER_CLASS_PROPERTY = "sqoop.minicluster.class";

  public static SqoopMiniCluster getSqoopMiniCluster(Properties properties, Class<? extends SqoopMiniCluster> defaultClusterClass, String temporaryPath, Configuration configuration, KdcRunner kdc) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
    String className = properties.getProperty(MINICLUSTER_CLASS_PROPERTY);
    Class<?> klass = className == null ? defaultClusterClass : Class.forName(className);
    Constructor konstructor = klass.getConstructor(String.class, Configuration.class);
    SqoopMiniCluster cluster = (SqoopMiniCluster)konstructor.newInstance(temporaryPath, configuration);
    cluster.setKdc(kdc);
    return cluster;
  }
}
