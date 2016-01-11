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
package org.apache.sqoop.test.kdc;

import java.util.Properties;

/**
 * Create KdcRunner.
 */
public class KdcRunnerFactory {

  public static final String KDC_CLASS_PROPERTY = "sqoop.kdc.runner.class";

  public static KdcRunner getKdc(Properties properties, Class<? extends KdcRunner> defaultClusterClass) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    String className = properties.getProperty(KDC_CLASS_PROPERTY);
    if(className == null) {
      return defaultClusterClass.newInstance();
    }

    Class<?> klass = Class.forName(className);
    return (KdcRunner)klass.newInstance();
  }
}
