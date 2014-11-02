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
package org.apache.sqoop.common.test.db;

import java.util.Properties;

/**
 * Create database provider.
 */
public class DatabaseProviderFactory {

  public static final String PROVIDER_PROPERTY = "sqoop.provider.class";

  public static DatabaseProvider getProvider(Properties properties) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    String className = properties.getProperty(PROVIDER_PROPERTY);
    if(className == null) {
      return new DerbyProvider();
    }

    Class<?> klass = Class.forName(className);
    return (DatabaseProvider)klass.newInstance();
  }

}
