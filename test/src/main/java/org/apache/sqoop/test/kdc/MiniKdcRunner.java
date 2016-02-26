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
import java.lang.reflect.Constructor;
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
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.apache.sqoop.test.utils.SecurityUtils;
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
  private String spnegoKeytabFile;

  // Currently all the services such as NameNode, DataNode, ResourceManager, NodeManager, JobHistoryServer,
  // Hive Metastore, Hive Server 2, Sqoop Server, etc login with the same principal as all the services are
  // running in the same JVM in the integration tests and the variable loginUser in the UserGroupInformation
  // which represents the currently login user is static.
  private String hadoopPrincipal;
  private String hadoopKeytabFile;

  @Override
  public Configuration prepareHadoopConfiguration(Configuration config) throws Exception {
    config.set("hadoop.security.authentication", "kerberos");

    // HDFS related configurations
    // NameNode configurations
    config.set("dfs.namenode.kerberos.principal", hadoopPrincipal);
    config.set("dfs.namenode.keytab.file", hadoopKeytabFile);
    config.set("dfs.namenode.kerberos.internal.spnego.principal", spnegoPrincipal);
    config.set("dfs.web.authentication.kerberos.principal", spnegoPrincipal);
    config.set("dfs.web.authentication.kerberos.keytab", spnegoKeytabFile);
    config.set("dfs.encrypt.data.transfer", "true");
    // DataNode configurations
    config.set("dfs.datanode.kerberos.principal", hadoopPrincipal);
    config.set("dfs.datanode.keytab.file", hadoopKeytabFile);
    String sslKeystoresDir = getTemporaryPath() + "/ssl-keystore";
    String sslConfDir = SqoopUtils.getClasspathDir(MiniKdcRunner.class);
    FileUtils.deleteDirectory(new File(sslKeystoresDir));
    FileUtils.forceMkdir(new File(sslKeystoresDir));
    SecurityUtils.setupSSLConfig(sslKeystoresDir, sslConfDir, config, false, true);
    config.set("dfs.https.server.keystore.resource", SecurityUtils.getSSLConfigFileName("ssl-server"));
    // Configurations used by both NameNode and DataNode
    config.set("dfs.block.access.token.enable", "true");
    config.set("dfs.http.policy", "HTTPS_ONLY");
    // Configurations used by DFSClient
    config.set("dfs.data.transfer.protection", "privacy");
    config.set("dfs.client.https.keystore.resource", SecurityUtils.getSSLConfigFileName("ssl-client"));

    // YARN related configurations
    config.set("yarn.resourcemanager.principal", hadoopPrincipal);
    config.set("yarn.resourcemanager.keytab", hadoopKeytabFile);
    config.set("yarn.resourcemanager.webapp.spnego-principal", spnegoPrincipal);
    config.set("yarn.resourcemanager.webapp.spnego-keytab-file", spnegoKeytabFile);
    config.set("yarn.nodemanager.principal", hadoopPrincipal);
    config.set("yarn.nodemanager.keytab", hadoopKeytabFile);

    // MapReduce related configurations
    config.set("mapreduce.jobhistory.principal", hadoopPrincipal);
    config.set("mapreduce.jobhistory.keytab", hadoopKeytabFile);
    config.set("yarn.app.mapreduce.am.command-opts",
        "-Xmx1024m -Djava.security.krb5.conf=\\\"" +
            miniKdc.getKrb5conf().getCanonicalPath() + "\\\"");
    config.set("mapred.child.java.opts",
        "-Xmx200m -Djava.security.krb5.conf=\\\"" +
            miniKdc.getKrb5conf().getCanonicalPath() + "\\\"");

    return config;
  }

  public Map<String, String> prepareSqoopConfiguration(Map<String, String> properties) {
    properties.put("org.apache.sqoop.security.authentication.type", "KERBEROS");
    properties.put("org.apache.sqoop.security.authentication.kerberos.http.principal", spnegoPrincipal);
    properties.put("org.apache.sqoop.security.authentication.kerberos.http.keytab", spnegoKeytabFile);

    // Sqoop Server do kerberos authentication with other services
    properties.put("org.apache.sqoop.security.authentication.handler", "org.apache.sqoop.security.authentication.KerberosAuthenticationHandler");
    properties.put("org.apache.sqoop.security.authentication.kerberos.principal", hadoopPrincipal);
    properties.put("org.apache.sqoop.security.authentication.kerberos.keytab", hadoopKeytabFile);
    return properties;
  }

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

  private static class KerberosConfiguration extends javax.security.auth.login.Configuration {
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

  private void createPrincipals() throws Exception {
    createSpnegoPrincipal();
    createSqoopPrincipals();
    createHadoopPrincipals();
  }

  /**
   * Create spnego principal which will be used by all the http servers.
   */
  private void createSpnegoPrincipal() throws Exception {
    String keytabDir = HdfsUtils.joinPathFragments(getTemporaryPath(), "spnego");
    File keytabDirFile = new File(keytabDir);
    FileUtils.deleteDirectory(keytabDirFile);
    FileUtils.forceMkdir(keytabDirFile);

    File keytabFile = new File(keytabDirFile, "HTTP.keytab");
    String host = SqoopUtils.getLocalHostName();
    miniKdc.createPrincipal(keytabFile, "HTTP/" + host);
    spnegoKeytabFile = keytabFile.getAbsolutePath();
    spnegoPrincipal = "HTTP/" + host + "@" + miniKdc.getRealm();
  }

  private void createSqoopPrincipals() throws Exception {
    String keytabDir = HdfsUtils.joinPathFragments(getTemporaryPath(), "sqoop");
    File keytabDirFile = new File(keytabDir);
    FileUtils.deleteDirectory(keytabDirFile);
    FileUtils.forceMkdir(keytabDirFile);

    String sqoopClientUserName = "sqoopclient";
    File userKeytabFile = new File(keytabDirFile, sqoopClientUserName + ".keytab");
    miniKdc.createPrincipal(userKeytabFile, sqoopClientUserName);
    sqoopClientPrincipal = sqoopClientUserName + "@" + miniKdc.getRealm();
    sqoopClientKeytabFile = userKeytabFile.getAbsolutePath();
  }

  private void createHadoopPrincipals() throws Exception {
    String keytabDir = HdfsUtils.joinPathFragments(getTemporaryPath(), "hadoop");
    File keytabDirFile = new File(keytabDir);
    FileUtils.deleteDirectory(keytabDirFile);
    FileUtils.forceMkdir(keytabDirFile);

    // Create principals for Hadoop, this principal will be used by all services
    // Reference SQOOP-2744 for detailed information
    String hadoopUserName = "hadoop";
    File keytabFile = new File(keytabDirFile, hadoopUserName + ".keytab");
    String host = SqoopUtils.getLocalHostName();
    miniKdc.createPrincipal(keytabFile, hadoopUserName + "/" + host);
    hadoopKeytabFile = keytabFile.getAbsolutePath();
    hadoopPrincipal = hadoopUserName + "/" + host + "@" + miniKdc.getRealm();
  }

  private <T> T doAsSqoopClient(Callable<T> callable) throws Exception {
    return doAs(sqoopClientPrincipal, sqoopClientKeytabFile, callable);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static <T> T doAs(String principal, String keytabFile, final Callable<T> callable) throws Exception {
    LoginContext loginContext = null;
    try {
      Set<Principal> principals = new HashSet<Principal>();
      Class userClass = Class.forName("org.apache.hadoop.security.User");
      Constructor<?> constructor = userClass.getDeclaredConstructor(String.class);
      constructor.setAccessible(true);
      principals.add((Principal)constructor.newInstance(principal));
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
