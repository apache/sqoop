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
package org.apache.sqoop.util.password;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Abstract class for describing password loader.
 */
abstract public class PasswordLoader {

  /**
   * Load password from given path.
   *
   * @param path Path to load password from.
   * @param configuration Configuration object.
   * @return Password
   * @throws IOException
   */
  public abstract String loadPassword(String path, Configuration configuration) throws IOException;

  /**
   * Callback that allows to clean up configuration properties out of the
   * configuration object that will be passed down to the mapreduce framework.
   *
   * It's defined by the Sqoop framework that after calling cleanUpConfiguration(),
   * method loadPassword() will never be called again.
   */
  public void cleanUpConfiguration(Configuration configuration) {
    // Default implementation is empty
  }
}
