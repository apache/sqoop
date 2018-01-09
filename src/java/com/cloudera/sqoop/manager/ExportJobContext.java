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

package com.cloudera.sqoop.manager;

import com.cloudera.sqoop.SqoopOptions;

import org.apache.sqoop.manager.ConnManager;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class ExportJobContext
    extends org.apache.sqoop.manager.ExportJobContext {

  public ExportJobContext(final String table, final String jar,
      final SqoopOptions opts) {
    super(table, jar, opts);
  }

  public void setConnManager(ConnManager mgr) {
    super.setConnManager(mgr);
  }

  public ConnManager getConnManager() {
    return (ConnManager)super.getConnManager();
  }
}

