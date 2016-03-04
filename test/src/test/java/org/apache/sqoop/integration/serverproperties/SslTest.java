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
package org.apache.sqoop.integration.serverproperties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.sqoop.security.SecurityConstants;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.KdcInfrastructureProvider;
import org.apache.sqoop.test.minicluster.JettySqoopMiniCluster;
import org.apache.sqoop.test.minicluster.SqoopMiniCluster;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.apache.sqoop.test.utils.SecurityUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Map;

import static org.testng.Assert.assertEquals;

@Infrastructure(dependencies = {HadoopInfrastructureProvider.class, DatabaseInfrastructureProvider.class, KdcInfrastructureProvider.class})
@Test(groups = {"no-real-cluster"})
public class SslTest extends SqoopTestCase {

  private SqoopMiniCluster sqoopMiniCluster;
  private SSLContext defaultSslContext;
  private HostnameVerifier defaultHostNameVerifier;
  private X509Certificate serverCertificate;
  private String sslKeystoreDir;

  private final String KEYSTORE_DIR = "ssltestkeystores/";

  public static class SslSqoopMiniCluster extends JettySqoopMiniCluster {

    private String keyStoreFilePath;
    private String keyStorePassword;
    private String keyStorePasswordGenerator;
    private String keyManagerPassword;
    private String keyManagerPasswordGenerator;

    public SslSqoopMiniCluster(String temporaryPath, Configuration configuration,
                               String keyStoreFilePath, String keyStorePassword,
                               String keyStorePasswordGenerator, String keyManagerPassword,
                               String keyManagerPasswordGenerator) throws Exception {
      super(temporaryPath, configuration);
      this.keyStoreFilePath = keyStoreFilePath;
      this.keyStorePassword = keyStorePassword;
      this.keyStorePasswordGenerator = keyStorePasswordGenerator;
      this.keyManagerPassword = keyManagerPassword;
      this.keyManagerPasswordGenerator = keyManagerPasswordGenerator;
    }

    @Override
    protected Map<String, String> getSecurityConfiguration() {
      Map<String, String> properties = super.getSecurityConfiguration();

      properties.put(SecurityConstants.TLS_ENABLED, String.valueOf(true));
      properties.put(SecurityConstants.TLS_PROTOCOL, "TLSv1.2");
      properties.put(SecurityConstants.KEYSTORE_LOCATION, keyStoreFilePath);
      properties.put(SecurityConstants.KEYSTORE_PASSWORD, keyStorePassword);
      properties.put(SecurityConstants.KEYSTORE_PASSWORD_GENERATOR, keyStorePasswordGenerator);
      properties.put(SecurityConstants.KEYMANAGER_PASSWORD, keyManagerPassword);
      properties.put(SecurityConstants.KEYMANAGER_PASSWORD_GENERATOR, keyManagerPasswordGenerator);

      return properties;
    }
  }

  @BeforeSuite
  public void createCertificates() throws Exception {
    sslKeystoreDir = getTemporaryPath() + KEYSTORE_DIR;
    serverCertificate = setupKeystore(sslKeystoreDir);
  }

  @BeforeMethod
  public void backupState() throws Exception {
    authToken = new DelegationTokenAuthenticatedURL.Token();
    defaultSslContext = SSLContext.getDefault();
    defaultHostNameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();
  }

  @BeforeMethod(dependsOnMethods = { "backupState" })
  public void bypassHostnameVerification() throws Exception {
    HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
      public boolean verify(String hostname, SSLSession session) {
        try {
          if (hostname.equals((new URL(sqoopMiniCluster.getServerUrl())).getHost())) {
            return true;
          }
        } catch (MalformedURLException e) {
          return false;
        }
        return false;
      }
    });
  }

  private void prepareKDC() {
    KdcInfrastructureProvider kdcProvider = getInfrastructureProvider(KdcInfrastructureProvider.class);
    if (kdcProvider != null) {
      sqoopMiniCluster.setKdc(kdcProvider.getInstance());
    }
  }

  private X509Certificate setupKeystore(String sslKeystoreDir) throws Exception {
    String sslConfDir = sslKeystoreDir;
    FileUtils.deleteDirectory(new File(sslKeystoreDir));
    FileUtils.forceMkdir(new File(sslKeystoreDir));
    return SecurityUtils.setupSSLConfig(
      sslKeystoreDir, sslConfDir, new Configuration(), false, true);
  }

  @AfterSuite
  public void deleteCertificates() throws Exception {
    FileUtils.deleteDirectory(new File(sslKeystoreDir));
  }

  @AfterMethod
  public void restoreState() {
    SSLContext.setDefault(defaultSslContext);
    HttpsURLConnection.setDefaultHostnameVerifier(defaultHostNameVerifier);
  }

  @AfterMethod
  public void stopCluster() throws Exception {
    try {
      sqoopMiniCluster.stop();
    } catch (Exception e) {
      throw e;
    }

  }

  @Test
  public void testSslInUseWithPassword() throws Exception {
    sqoopMiniCluster =
      new SslSqoopMiniCluster(HdfsUtils.joinPathFragments(getTemporaryPath(), getTestName()),
        getHadoopConf(), sslKeystoreDir + SecurityUtils.SERVER_KEYSTORE,
        SecurityUtils.SERVER_KEY_STORE_PASSWORD, "",
        SecurityUtils.SERVER_KEY_PASSWORD, "");

    prepareKDC();

    sqoopMiniCluster.start();

    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(sslKeystoreDir + SecurityUtils.TRUSTSTORE);
    sslContextFactory.start();
    SSLContext.setDefault(sslContextFactory.getSslContext());

    initSqoopClient(sqoopMiniCluster.getServerUrl());

    verifySsl(serverCertificate);
  }

  @Test
  public void testSslInUseWithPasswordGenerator() throws Exception {
    sqoopMiniCluster =
      new SslSqoopMiniCluster(HdfsUtils.joinPathFragments(getTemporaryPath(), getTestName()),
        getHadoopConf(), sslKeystoreDir + SecurityUtils.SERVER_KEYSTORE,
        "", "echo " + SecurityUtils.SERVER_KEY_STORE_PASSWORD,
        "", "echo " + SecurityUtils.SERVER_KEY_PASSWORD );

    prepareKDC();

    sqoopMiniCluster.start();

    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(sslKeystoreDir + SecurityUtils.TRUSTSTORE);
    sslContextFactory.start();

    SSLContext.setDefault(sslContextFactory.getSslContext());

    initSqoopClient(sqoopMiniCluster.getServerUrl());
    verifySsl(serverCertificate);
  }

  private void verifySsl(X509Certificate serverCertificate) throws Exception {
    URL url = new URL(sqoopMiniCluster.getServerUrl() + "version?" +
      PseudoAuthenticator.USER_NAME + "=" + System.getProperty("user.name"));

    HttpURLConnection conn = new DelegationTokenAuthenticatedURL().openConnection(url, getAuthToken());
    conn.setRequestMethod(HttpMethod.GET);
    conn.setRequestProperty("Accept", MediaType.APPLICATION_JSON);

    assertEquals(conn.getResponseCode(), 200);

    HttpsURLConnection secured = (HttpsURLConnection) conn;

    Certificate actualCertificate = secured.getServerCertificates()[0];

    secured.disconnect();
    assertEquals(actualCertificate, serverCertificate);
  }

}
