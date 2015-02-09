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
package org.apache.sqoop.model;

/**
 * Model describing entire resource object which used in resource based authorization controller
 */
public class MResource {

  private final String name;
  /**
   * Currently, the type supports connector, link, job and submission.
   */
  private final String type;

  /**
   * Default constructor to build  new MResource model.
   *
   * @param name Resource name
   * @param type Resource type
   */
  public MResource(String name,
                   String type) {
    this.name = name;
    this.type = type;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Resource (");
    sb.append("Resource name: ").append(this.name);
    sb.append(", Resource type: ").append(this.type);
    sb.append(" )");

    return sb.toString();
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }
}
