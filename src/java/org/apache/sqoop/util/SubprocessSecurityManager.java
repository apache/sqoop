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

package org.apache.sqoop.util;

import java.security.Permission;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A SecurityManager used to run subprocesses and disallow certain actions.
 *
 * This specifically disallows System.exit().
 *
 * This SecurityManager will also check with any existing SecurityManager as
 * to the validity of any permissions. The SubprocessSecurityManager should be
 * installed with the install() method, which will retain a handle to any
 * previously-installed SecurityManager instance.
 *
 * When this SecurityManager is no longer necessary, the uninstall() method
 * should be used which reinstates the previous SecurityManager as the active
 * SecurityManager.
 */
public class SubprocessSecurityManager extends SecurityManager {

  public static final Log LOG = LogFactory.getLog(
      SubprocessSecurityManager.class.getName());

  private SecurityManager parentSecurityManager;
  private boolean installed;
  private boolean allowReplacement;

  public SubprocessSecurityManager() {
    this.installed = false;
    this.allowReplacement = false;
  }

  /**
   * Install this SecurityManager and retain a reference to any
   * previously-installed SecurityManager.
   */
  public void install() {
    LOG.debug("Installing subprocess security manager");
    this.parentSecurityManager = System.getSecurityManager();
    System.setSecurityManager(this);
    this.installed = true;
  }

  /**
   * Restore an existing SecurityManager, uninstalling this one.
   */
  public void uninstall() {
    if (this.installed) {
      LOG.debug("Uninstalling subprocess security manager");
      this.allowReplacement = true;
      System.setSecurityManager(this.parentSecurityManager);
    }
  }

  @Override
  /**
   * Disallow the capability to call System.exit() or otherwise
   * terminate the JVM.
   */
  public void checkExit(int status) {
    LOG.debug("Rejecting System.exit call with status=" + status);
    throw new com.cloudera.sqoop.util.ExitSecurityException(status);
  }

  @Override
  /**
   * Check a particular permission. Checks with this SecurityManager
   * as well as any previously-installed manager.
   *
   * @param perm the Permission to check; must not be null.
   */
  public void checkPermission(Permission perm) {
    if (null != this.parentSecurityManager) {
      // Check if the prior SecurityManager would have rejected this.
      parentSecurityManager.checkPermission(perm);
    }

    if (!allowReplacement && perm.getName().equals("setSecurityManager")) {
      throw new SecurityException("Cannot replace security manager");
    }
  }

}

