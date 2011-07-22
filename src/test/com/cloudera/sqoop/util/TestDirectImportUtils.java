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

package com.cloudera.sqoop.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Test that /etc/hosts is configured sanely.
 * If this fails, then the postgresql direct tests will likely fail.
 *
 * In particular, 'localhost' needs to resolve to 127.0.0.1.
 * The /etc/hosts file should contain a line like:
 *
 * 127.0.0.1  (yourhostnamehere) localhost
 *
 * e.g.:
 * 127.0.0.1  aaron-laptop localhost
 * 127.0.1.1  aaron-laptop
 * ...
 *
 * If it doesn't have 'localhost' on that line, it'll fail.
 */
public class TestDirectImportUtils extends TestCase {

  public static final Log LOG = LogFactory.getLog(
        TestDirectImportUtils.class.getName());
        
  public void testLocalhost() throws UnknownHostException {
    InetAddress localHostAddr = InetAddress.getLocalHost();
    LOG.info("Advertised localhost address: " + localHostAddr);

    InetAddress requestAddr = InetAddress.getByName("localhost");
    LOG.info("Requested localhost address: " + requestAddr);

    assertTrue("Requested addr does not identify as localhost. "
        + "Check /etc/hosts (see TestDirectImportUtils comments)",
        localHostAddr.equals(requestAddr));

    assertTrue("Couldn't match 'localhost' to localhost; "
        + "check /etc/hosts (see TestDirectImportUtils comments)",
        DirectImportUtils.isLocalhost("localhost"));
  }
}

