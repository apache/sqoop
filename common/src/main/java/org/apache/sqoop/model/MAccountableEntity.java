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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

import java.util.Date;

/**
 * Accountable entity provides additional fields that might help with identifying
 * what and when has happened.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
abstract public class MAccountableEntity extends MValidatedElement {

  private final boolean DEFAULT_ENABLED = true;

  /**
   * The user who creates the entity
   */
  private String creationUser;

  /**
   * Date when the entity was created.
   */
  private Date creationDate;

  /**
   * The user who lastly updates the entity
   */
  private String lastUpdateUser;

  /**
   * Date when the entity was lastly updated.
   */
  private Date lastUpdateDate;

  /**
   * Whether enabled
   */
  private boolean enabled;

  /**
   * Default constructor.
   *
   * Set creation and last update date to now and users as null. By default
   * the accountable entity is enabled.
   */
  public MAccountableEntity() {
    super((String)null);
    this.creationUser = null;
    this.creationDate = new Date();
    this.lastUpdateUser = this.creationUser;
    this.lastUpdateDate = this.creationDate;
    this.enabled = DEFAULT_ENABLED;
  }

  /**
   * Create new accountable entity as copy of other accountable entity.
   *
   * @param other Accountable entity to copy
   */
  public MAccountableEntity(MAccountableEntity other) {
    super(other);
    this.creationDate = other.creationDate;
    this.creationUser = other.creationUser;
    this.lastUpdateDate = other.lastUpdateDate;
    this.lastUpdateUser = other.lastUpdateUser;
    this.enabled = other.enabled;
  }

  public void setCreationUser(String name) {
    this.creationUser = name;
  }

  public String getCreationUser() {
    return creationUser;
  }

  public void setCreationDate(Date createDate) {
    this.creationDate = createDate;
  }

  public Date getCreationDate() {
    return creationDate;
  }

  public void setLastUpdateUser(String name) {
    this.lastUpdateUser = name;
  }

  public String getLastUpdateUser() {
    return lastUpdateUser;
  }

  public void setLastUpdateDate(Date lastUpdateDate) {
    this.lastUpdateDate = lastUpdateDate;
  }

  public Date getLastUpdateDate() {
    return lastUpdateDate;
  }

  public void setEnabled(boolean enable) {
    this.enabled = enable;
  }

  public boolean getEnabled() {
    return this.enabled;
  }
}
