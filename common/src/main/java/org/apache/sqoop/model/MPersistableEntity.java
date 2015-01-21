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

/**
 * Represents a persistable metadata entity.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class MPersistableEntity {

  public static final long PERSISTANCE_ID_DEFAULT = -1L;

  private long persistenceId = PERSISTANCE_ID_DEFAULT;

  /**
   * Default constructor.
   */
  protected MPersistableEntity() {
  }

  /**
   * Constructor building as a copy of other persistable entity.
   *
   * @param other Persistable entity to copy
   */
  protected MPersistableEntity(MPersistableEntity other) {
    this.persistenceId = other.persistenceId;
  }

  public void setPersistenceId(long persistenceId) {
    this.persistenceId = persistenceId;
  }

  public long getPersistenceId() {
    return persistenceId;
  }

  public boolean hasPersistenceId() {
    return persistenceId != PERSISTANCE_ID_DEFAULT;
  }

  @Override
  public abstract String toString();
}
