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
import org.apache.sqoop.test.utils.SqoopUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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

@Infrastructure(dependencies = {KdcInfrastructureProvider.class, HadoopInfrastructureProvider.class, DatabaseInfrastructureProvider.class})
@Test(groups = {"no-real-cluster"})
public class SslTest extends SqoopTestCase {

  private SqoopMiniCluster sqoopMiniCluster;
  private SSLContext defaultSslContext;
  private HostnameVerifier defaultHostNameVerifier;

  public static class SslSqoopMiniCluster extends JettySqoopMiniCluster {

    private String keyStoreFilePath;
    private String keyStorePassword;
    private String keyManagerPassword;

    public SslSqoopMiniCluster(String temporaryPath, Configuration configuration, String keyStoreFilePath, String keyStorePassword, String keyManagerPassword) throws Exception {
      super(temporaryPath, configuration);
      this.keyStoreFilePath = keyStoreFilePath;
      this.keyStorePassword = keyStorePassword;
      this.keyManagerPassword = keyManagerPassword;
    }

    @Override
    protected Map<String, String> getSecurityConfiguration() {
      Map<String, String> properties = super.getSecurityConfiguration();

      properties.put(SecurityConstants.TLS_ENABLED, String.valueOf(true));
      properties.put(SecurityConstants.TLS_PROTOCOL, "TLSv1.2");
      properties.put(SecurityConstants.KEYSTORE_LOCATION, keyStoreFilePath);
      properties.put(SecurityConstants.KEYSTORE_PASSWORD, keyStorePassword);
      properties.put(SecurityConstants.KEYMANAGER_PASSWORD, keyManagerPassword);

      return properties;
    }
  }

  @BeforeMethod
  public void backupSslContext() throws Exception {
    defaultSslContext = SSLContext.getDefault();
    defaultHostNameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();
  }

  @AfterMethod
  public void restoreSslContext() {
    SSLContext.setDefault(defaultSslContext);
    HttpsURLConnection.setDefaultHostnameVerifier(defaultHostNameVerifier);
  }

  @AfterMethod
  public void stopCluster() throws Exception {
    sqoopMiniCluster.stop();
  }

  @Test
  public void testSslInUse() throws Exception {
    String sslKeystoresDir = getTemporaryPath() + "ssl-keystore/";
    String sslConfDir = SqoopUtils.getClasspathDir(SslTest.class);
    FileUtils.deleteDirectory(new File(sslKeystoresDir));
    FileUtils.forceMkdir(new File(sslKeystoresDir));
    X509Certificate serverCertificate = SecurityUtils.setupSSLConfig(
      sslKeystoresDir, sslConfDir, new Configuration(), false, true);

    sqoopMiniCluster =
      new SslSqoopMiniCluster(HdfsUtils.joinPathFragments(getTemporaryPath(), getTestName()), getHadoopConf(), sslKeystoresDir + SecurityUtils.SERVER_KEYSTORE, SecurityUtils.SERVER_KEY_STORE_PASSWORD, SecurityUtils.SERVER_KEY_PASSWORD);

    KdcInfrastructureProvider kdcProvider = getInfrastructureProvider(KdcInfrastructureProvider.class);
    if (kdcProvider != null) {
      sqoopMiniCluster.setKdc(kdcProvider.getInstance());
    }

    sqoopMiniCluster.start();

    // Bypass hostname verification
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

    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(sslKeystoresDir + SecurityUtils.TRUSTSTORE);

    sslContextFactory.start();

    SSLContext.setDefault(sslContextFactory.getSslContext());

    initSqoopClient(sqoopMiniCluster.getServerUrl());

    // Make a request and check the cert
    URL url = new URL(sqoopMiniCluster.getServerUrl() + "version?" +
      PseudoAuthenticator.USER_NAME + "=" + System.getProperty("user.name"));
    HttpURLConnection conn = new DelegationTokenAuthenticatedURL().openConnection(url, getAuthToken());
    conn.setRequestMethod(HttpMethod.GET);
    conn.setRequestProperty("Accept", MediaType.APPLICATION_JSON);

    assertEquals(conn.getResponseCode(), 200);

    HttpsURLConnection secured = (HttpsURLConnection) conn;
    Certificate actualCertificate = secured.getServerCertificates()[0];
    assertEquals(actualCertificate, serverCertificate);
  }

}
