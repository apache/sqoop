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
package org.apache.sqoop.docs.generator.plugins;

/**
 * Abstract plugin class to abstract steps during docs pre-processing
 */
abstract public class AbstractPlugin {

  /**
   * Source directory
   */
  private String source;

  /**
   * Destination directory;
   */
  private String destination;

  /**
   * Do the plugin step.
   *
   * Throws RuntimeException aborting the build on error.
   */
  abstract public void run();

  public void setSource(String source) {
    this.source = source;
  }

  public String getSource() {
    return source;
  }

  public void setDestination(String destination) {
    this.destination = destination;
  }

  public String getDestination() {
    return destination;
  }

}
