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

import java.util.Date;

/**
 * Accountable entity provides additional fields that might help with identifying
 * what and when has happened.
 */
abstract public class MAccountableEntity extends MPersistableEntity {

  /**
   * Date when the entity was created.
   */
  private Date creationDate;

  /**
   * Date when the entity was lastly updated.
   */
  private Date lastUpdateDate;

  public MAccountableEntity() {
    this.creationDate = new Date();
    this.lastUpdateDate = this.creationDate;
  }

  public void setCreationDate(Date createDate) {
    this.creationDate = createDate;
  }

  public Date getCreationDate() {
    return creationDate;
  }

  public void setLastUpdateDate(Date lastUpdateDate) {
    this.lastUpdateDate = lastUpdateDate;
  }

  public Date getLastUpdateDate() {
    return lastUpdateDate;
  }
}
