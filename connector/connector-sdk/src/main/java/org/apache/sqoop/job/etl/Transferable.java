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
package org.apache.sqoop.job.etl;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * This entity encapsulates the workflow for data transfer via the
 * {@link SqoopConnector}.It basically acts as an adapter between the data-source
 * imported from or exported to.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Transferable {

  private Class<? extends Initializer> initializer;
  private Class<? extends Destroyer> destroyer;

  public Transferable(
    Class<? extends Initializer> initializer,
    Class<? extends Destroyer> destroyer
  ) {
    this.initializer = initializer;
    this.destroyer = destroyer;
  }

  public Class<? extends Destroyer> getDestroyer() {
    return destroyer;
  }

  public Class<? extends Initializer> getInitializer() {
    return initializer;
  }

  @Override
  public String toString() {
    return "initializer=" + initializer.getName() +
            ", destroyer=" + destroyer.getName();
  }
}