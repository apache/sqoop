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
package org.apache.sqoop.test.infrastructure.providers;

import org.apache.hadoop.conf.Configuration;

/**
 * Infrastructure classes enable the development of integration tests.
 * These are usually scripts to start miniclusters.
 */
public abstract class InfrastructureProvider {
  /**
   * Start miniclusters.
   */
  abstract public void start();

  /**
   * Stop miniclusters.
   */
  abstract public void stop();

  /**
   * Set Hadoop configuration to be used.
   *
   * Called before ``start()`` method.
   *
   * @param conf
   */
  abstract public void setHadoopConfiguration(Configuration conf);

  /**
   * @return hadoop configuration used
   */
  abstract public Configuration getHadoopConfiguration();

  /**
   * Set the root path to be used by this infrastructure component.
   * This is intended to provide some level of containment.
   *
   * Called before ``start()`` method.
   *
   * @param path
   */
  abstract public void setRootPath(String path);

  /**
   * @return root path for component.
   */
  abstract public String getRootPath();
}