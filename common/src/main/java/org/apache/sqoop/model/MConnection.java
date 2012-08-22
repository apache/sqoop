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
 * Model describing entire connection object including both connector and
 * framework part.
 */
public class MConnection extends MNamedElement {
  // TODO(jarcec): We probably need reference to connector object here
  MConnectionForms connectorPart;
  MConnectionForms frameworkPart;

  public MConnection(String name) {
    super(name);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("connection connector-part: ");
    sb.append(connectorPart).append(", framework-part: ").append(frameworkPart);

    return sb.toString();
  }
}
