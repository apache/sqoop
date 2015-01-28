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
 * Model describing entire privilege object which used in privilege based authorization controller
 */
public class MPrivilege {

  private final String id;
  private final String name;
  private final MResource resource;
  /**
   * Currently, the action supports view, use, create, update, delete and enable_disable.
   */
  private final String action;
  private final boolean with_grant_option;


  /**
   * Default constructor to build  new MPrivilege model.
   *
   * @param id                Privilege id
   * @param name              Privilege name
   * @param resource          Privilege resource
   * @param action            Privilege action
   * @param with_grant_option Privilege with_grant_option
   */
  public MPrivilege(String id,
                    String name,
                    MResource resource,
                    String action,
                    boolean with_grant_option) {
    this.id = id;
    this.name = name;
    this.resource = resource;
    this.action = action;
    this.with_grant_option = with_grant_option;
  }

  /**
   * Constructor to build  new MPrivilege model.
   *
   * @param name     Privilege name
   * @param resource Privilege resource
   * @param action   Privilege action
   */
  public MPrivilege(String name,
                    MResource resource,
                    String action) {
    this(null, name, resource, action, false);
  }

  /**
   * Constructor to build  new MPrivilege model.
   *
   * @param resource Privilege resource
   * @param action   Privilege action
   */
  public MPrivilege(MResource resource,
                    String action) {
    this(null, resource, action);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Privilege (");
    sb.append("Privilege id: ").append(this.id);
    sb.append(", Privilege name: ").append(this.name);
    sb.append(", Privilege resource: ").append(this.getResource().toString());
    sb.append(", Privilege action: ").append(this.action);
    sb.append(", Privilege with_grant_option: ").append(this.with_grant_option);
    sb.append(" )");

    return sb.toString();
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public MResource getResource() {
    return resource;
  }

  public String getAction() {
    return action;
  }

  public boolean isWith_grant_option() {
    return with_grant_option;
  }
}
