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
package org.apache.sqoop.util.password;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Helper class for the Hadoop credential provider functionality.
 * Reflrection to used to avoid directly referencing the classes and methods
 * so that version dependency is not introduced as the Hadoop credential
 * provider is only introduced in 2.6.0 and later
 */
public class CredentialProviderHelper {
  public static final Log LOG =
    LogFactory.getLog(CredentialProviderHelper.class.getName());

  private static Class<?> clsCredProvider;
  private static Class<?> clsCredProviderFactory;
  private static Method methGetPassword;
  private static Method methGetProviders;
  private static Method methCreateCredEntry;
  private static Method methFlush;

  static {
    try {
      LOG.debug("Reflecting credential provider classes and methods");
      clsCredProvider = Class
        .forName("org.apache.hadoop.security.alias.CredentialProvider");
      LOG
        .debug("Found org.apache.hadoop.security.alias.CredentialProvider");
      clsCredProviderFactory = Class.forName(
        "org.apache.hadoop.security.alias.CredentialProviderFactory");
      LOG
        .debug("Found org.apache.hadoop.security.alias.CredentialProviderFactory");

      methCreateCredEntry = clsCredProvider.getMethod("createCredentialEntry",
        new Class[] { String.class, char[].class });
      LOG
        .debug("Found CredentialProvider#createCredentialEntry");

      methFlush = clsCredProvider.getMethod("flush",
        new Class[] {});
      LOG
        .debug("Found CredentialProvider#flush");

      methGetPassword = Configuration.class.getMethod("getPassword",
        new Class[] { String.class });
      LOG
        .debug("Found Configuration#getPassword");

      methGetProviders = clsCredProviderFactory.getMethod("getProviders",
        new Class[] { Configuration.class });
      LOG
        .debug("Found CredentialProviderFactory#getProviders");
    } catch (ClassNotFoundException cnfe) {
      LOG.debug("Ignoring exception", cnfe);
    } catch (NoSuchMethodException nsme) {
      LOG.debug("Ignoring exception", nsme);
    }
  }
  // Should track what is specified in JavaKeyStoreProvider class.
  public static final String SCHEME_NAME = "jceks";
  // Should track what is in CredentialProvider class.
  public static final String CREDENTIAL_PROVIDER_PATH =
    "hadoop.security.credential.provider.path";

  public static boolean isProviderAvailable() {

    if (clsCredProvider == null
      || clsCredProviderFactory == null
      || methCreateCredEntry == null
      || methGetPassword == null
      || methFlush == null) {
      return false;
    }
    return true;
  }

  public static String resolveAlias(Configuration conf, String alias)
    throws IOException {
    LOG.debug("Resolving alias with credential provider path set to "
      + conf.get(CREDENTIAL_PROVIDER_PATH));
    try {
      char[] cred = (char[])
        methGetPassword.invoke(conf, new Object[] { alias });
      if (cred == null) {
        throw new IOException("The provided alias cannot be resolved");
      }
      String pass = new String(cred);
      return pass;
    } catch (InvocationTargetException ite) {
      throw new RuntimeException("Error resolving password "
        + " from the credential providers ", ite.getTargetException());
    } catch (IllegalAccessException iae) {
      throw new RuntimeException("Error invoking the credential provider method",
        iae);
    }
  }
  /**
   * Test utility to create an entry
   */
  public static void createCredentialEntry(Configuration conf,

    String alias, String credential) throws IOException {

    if (!isProviderAvailable()) {
      throw new RuntimeException("CredentialProvider facility not available "
        + "in the hadoop environment");
    }


    try {
      List<?> result = (List<?>)
        methGetProviders.invoke(null, new Object[] { conf });
      Object provider = result.get(0);
      LOG.debug("Using credential provider " + provider);

      methCreateCredEntry.invoke(provider, new Object[] {
        alias, credential.toCharArray() });
      methFlush.invoke(provider, new Object[] {});
    } catch (InvocationTargetException ite) {
      throw new RuntimeException("Error creating credential entry "
        + " using the credentail provider", ite.getTargetException());
    } catch (IllegalAccessException iae) {
      throw new RuntimeException("Error accessing the credential create method",
        iae);
    }
  }
}
