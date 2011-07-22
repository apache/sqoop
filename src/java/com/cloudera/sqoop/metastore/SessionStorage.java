/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.metastore;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configured;

/**
 * API that defines how sessions are saved, restored, and manipulated.
 *
 * <p>
 * SessionStorage instances may be created and then not used; the
 * SessionStorage factory may create additional SessionStorage instances
 * that return false from accept() and then discard them. The close()
 * method will only be triggered for a SessionStorage if the connect()
 * method is called. Connection should not be triggered by a call to
 * accept().</p>
 */
public abstract class SessionStorage extends Configured implements Closeable {

  /**
   * Returns true if the SessionStorage system can use the metadata in
   * the descriptor to connect to an underlying session resource.
   */
  public abstract boolean canAccept(Map<String, String> descriptor);


  /**
   * Opens / connects to the underlying storage resource specified by the
   * descriptor.
   */
  public abstract void open(Map<String, String> descriptor)
      throws IOException;

  /**
   * Given a session name, reconstitute a SessionData that contains all
   * configuration information required for the session. Returns null if the
   * session name does not match an available session.
   */
  public abstract SessionData read(String sessionName)
      throws IOException;

  /**
   * Forget about a saved session.
   */
  public abstract void delete(String sessionName) throws IOException;

  /**
   * Given a session name and the data describing a configured
   * session, record the session information to the storage medium.
   */
  public abstract void create(String sessionName, SessionData data)
      throws IOException;

  /**
   * Given a session descriptor and a configured session
   * update the underlying resource to match the current session
   * configuration.
   */
  public abstract void update(String sessionName, SessionData data)
      throws IOException;
  
  /**
   * Close any resources opened by the SessionStorage system.
   */
  public void close() throws IOException {
  }

  /**
   * Enumerate all sessions held in the connected resource.
   */
  public abstract List<String> list() throws IOException;
}

