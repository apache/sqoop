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

package org.apache.sqoop.common.test.repository;

import java.util.Map;

/**
 * Instantiate the right class for handling repository specific properties
 */
public class RepositoryProviderFactory {

  public static final String REPOSITORY_CLASS_PROPERTY = "sqoop.repository.handler.class";

  public static Map<String, String> getRepositoryProperties() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    String className = System.getProperty(REPOSITORY_CLASS_PROPERTY);
    RepositoryProviderBase repoProvider;
    if(className == null) {
      repoProvider = new DerbyRepositoryProvider();
    } else {
      Class<?> klass = Class.forName(className);
      repoProvider = (RepositoryProviderBase) klass.newInstance();
    }
    return repoProvider.getPropertiesMap();
  }
}
