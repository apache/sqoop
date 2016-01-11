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
package org.apache.sqoop.test.kdc;

import java.io.File;
import java.net.URL;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.apache.sqoop.test.utils.SqoopUtils;

/**
 * Represents a Minikdc setup. Minikdc should be only used together with
 * mini clusters such as JettySqoopMiniCluster, HadoopMiniClusterRunner,
 * InternalHiveServerRunner, InternalMetastoreServerRunner, etc.
 * It should not be used with real cluster.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"SIC_INNER_SHOULD_BE_STATIC_ANON"})
public class MiniKdcRunner extends KdcRunner {

  private MiniKdc miniKdc;

  private String sqoopClientPrincipal;
  private String sqoopClientKeytabFile;

  private String spnegoPrincipal;
  private String sqoopServerKeytabFile;

  @Override
  public void start() throws Exception {
    Properties kdcConf = MiniKdc.createConf();
    File baseDir = new File(getTemporaryPath(), "minikdc");
    FileUtils.deleteDirectory(baseDir);
    FileUtils.forceMkdir(baseDir);
    miniKdc = new MiniKdc(kdcConf, baseDir);
    miniKdc.start();

    createPrincipals();
  }

  @Override
  public void stop() throws Exception {
    miniKdc.stop();
  }

  public MiniKdc getMiniKdc() {
    return miniKdc;
  }

  @Override
  public String getSpnegoPrincipal() {
    return spnegoPrincipal;
  }

  @Override
  public String getSqoopServerKeytabFile() {
    return sqoopServerKeytabFile;
  }

  private static class KerberosConfiguration extends Configuration {
    private String principal;
    private String keytabFile;

    public KerberosConfiguration(String principal, String keytabFile) {
      this.principal = principal;
      this.keytabFile = keytabFile;
    }

    private String getKrb5LoginModuleName() {
      return System.getProperty("java.vendor").contains("IBM")
          ? "com.ibm.security.auth.module.Krb5LoginModule"
          : "com.sun.security.auth.module.Krb5LoginModule";
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();
      if (System.getProperty("java.vendor").contains("IBM")) {
        options.put("useKeytab", keytabFile.startsWith("file://") ? keytabFile : "file://" +  keytabFile);
        options.put("principal", principal);
        options.put("refreshKrb5Config", "true");
        options.put("credsType", "both");
      } else {
        options.put("keyTab", keytabFile);
        options.put("principal", principal);
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
        options.put("doNotPrompt", "true");
        options.put("useTicketCache", "true");
        options.put("renewTGT", "true");
        options.put("refreshKrb5Config", "true");
        options.put("isInitiator", "true");
      }
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        if (System.getProperty("java.vendor").contains("IBM")) {
          // IBM JAVA only respect system property and not env variable
          // The first value searched when "useDefaultCcache" is used.
          System.setProperty("KRB5CCNAME", ticketCache);
          options.put("useDefaultCcache", "true");
          options.put("renewTGT", "true");
        } else {
          options.put("ticketCache", ticketCache);
        }
      }
      options.put("debug", "true");

      return new AppConfigurationEntry[]{
        new AppConfigurationEntry(getKrb5LoginModuleName(),
                                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                  options),};
    }
  }

  @Override
  public void authenticateWithSqoopServer(final SqoopClient client) throws Exception {
    doAsSqoopClient(new Callable<Collection<MConnector>>() {
      @Override
      public Collection<MConnector> call() {
        return client.getConnectors();
      }
    });
  }

  @Override
  public void authenticateWithSqoopServer(final URL url, final DelegationTokenAuthenticatedURL.Token authToken) throws Exception {
    doAsSqoopClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        new DelegationTokenAuthenticatedURL().openConnection(url, authToken);
        return null;
      }
    });
  }

  @Override
  public boolean isKerberosEnabled() {
    return true;
  }

  private void createPrincipals() throws Exception {
    createPrincipalsForSqoopClient();
    createPrincipalsForSqoopServer();
  }

  private void createPrincipalsForSqoopClient() throws Exception {
    String keytabDir = HdfsUtils.joinPathFragments(getTemporaryPath(), "sqoop-client");
    File keytabDirFile = new File(keytabDir);
    FileUtils.deleteDirectory(keytabDirFile);
    FileUtils.forceMkdir(keytabDirFile);

    String userName = "sqoopclient";
    File userKeytabFile = new File(keytabDirFile, userName + ".keytab");
    miniKdc.createPrincipal(userKeytabFile, userName);
    sqoopClientPrincipal = userName + "@" + miniKdc.getRealm();
    sqoopClientKeytabFile = userKeytabFile.getAbsolutePath();
  }

  private void createPrincipalsForSqoopServer() throws Exception {
    String keytabDir = HdfsUtils.joinPathFragments(getTemporaryPath(), "sqoop-server");
    File keytabDirFile = new File(keytabDir);
    FileUtils.deleteDirectory(keytabDirFile);
    FileUtils.forceMkdir(keytabDirFile);

    String sqoopUserName = "sqoopserver";
    File sqoopKeytabFile = new File(keytabDirFile, sqoopUserName + ".keytab");
    String host = SqoopUtils.getLocalHostName();
    miniKdc.createPrincipal(sqoopKeytabFile, "HTTP/" + host);
    sqoopServerKeytabFile = sqoopKeytabFile.getAbsolutePath();
    spnegoPrincipal = "HTTP/" + host + "@" + miniKdc.getRealm();
  }

  private <T> T doAsSqoopClient(Callable<T> callable) throws Exception {
    return doAs(sqoopClientPrincipal, sqoopClientKeytabFile, callable);
  }

  private static <T> T doAs(String principal, String keytabFile, final Callable<T> callable) throws Exception {
    LoginContext loginContext = null;
    try {
      Set<Principal> principals = new HashSet<Principal>();
      principals.add(new KerberosPrincipal(principal));
      Subject subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());
      loginContext = new LoginContext("", subject, null, new KerberosConfiguration(principal, keytabFile));
      loginContext.login();
      subject = loginContext.getSubject();
      return Subject.doAs(subject, new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return callable.call();
        }
      });
    } catch (PrivilegedActionException ex) {
      throw ex.getException();
    } finally {
      if (loginContext != null) {
        loginContext.logout();
      }
    }
  }
}
