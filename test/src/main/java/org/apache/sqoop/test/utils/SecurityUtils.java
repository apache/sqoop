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
package org.apache.sqoop.test.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.bouncycastle.x509.X509V1CertificateGenerator;

import javax.security.auth.x500.X500Principal;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SecurityUtils {

  public static final String CLIENT_KEYSTORE = "clientKS.jks";
  public static final String SERVER_KEYSTORE = "serverKS.jks";
  public static final String TRUSTSTORE = "trustKS.jks";

  public static final String CLIENT_KEY_PASSWORD = "client_key";
  public static final String CLIENT_KEY_STORE_PASSWORD = "client_keystore";
  public static final String SERVER_KEY_PASSWORD = "server_key";
  public static final String SERVER_KEY_STORE_PASSWORD = "server_keystore";

  /**
   * Performs complete setup of SSL configuration. This includes keys, certs,
   * keystores, truststores, the server SSL configuration file,
   * the client SSL configuration file.
   *
   * @param keystoresDir String directory to save keystores
   * @param sslConfDir String directory to save SSL configuration files
   * @param conf Configuration
   * @param useClientCert boolean true to make the client present a cert in the
   * SSL handshake
   * @param trustStore boolean true to create truststore, false not to create it
   */
  public static X509Certificate setupSSLConfig(String keystoresDir, String sslConfDir,
                                    Configuration conf, boolean useClientCert, boolean trustStore)
    throws Exception {
    String clientKeyStorePath = keystoresDir + "/" + CLIENT_KEYSTORE;
    String serverKeyStorePath = keystoresDir + "/" + SERVER_KEYSTORE;
    String serverPassword = "serverP";
    String trustKS = null;
    String trustPassword = "trustP";

    File sslClientConfFile = new File(sslConfDir, getSSLConfigFileName("ssl-client"));
    File sslServerConfFile = new File(sslConfDir, getSSLConfigFileName("ssl-server"));

    Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();

    String hostname = SqoopUtils.getLocalHostName();

    if (useClientCert) {
      KeyPair cKP = generateKeyPair("RSA");
      X509Certificate cCert = generateCertificate("CN=" + hostname + ", O=client", cKP, 30, "SHA1withRSA");
      createKeyStore(clientKeyStorePath, CLIENT_KEY_PASSWORD, CLIENT_KEY_STORE_PASSWORD, "client", cKP.getPrivate(), cCert);
      certs.put("client", cCert);
    }

    KeyPair sKP = generateKeyPair("RSA");
    X509Certificate sCert = generateCertificate("CN=" + hostname + ", O=server", sKP, 30, "SHA1withRSA");
    createKeyStore(serverKeyStorePath, SERVER_KEY_PASSWORD, SERVER_KEY_STORE_PASSWORD, "server", sKP.getPrivate(), sCert);
    certs.put("server", sCert);

    if (trustStore) {
      trustKS = keystoresDir + TRUSTSTORE;
      createTrustStore(trustKS, trustPassword, certs);
    }

    Configuration clientSSLConf = createSSLConfig(
      SSLFactory.Mode.CLIENT, clientKeyStorePath, CLIENT_KEY_STORE_PASSWORD, CLIENT_KEY_PASSWORD, trustKS);
    Configuration serverSSLConf = createSSLConfig(
      SSLFactory.Mode.SERVER, serverKeyStorePath, SERVER_KEY_STORE_PASSWORD, SERVER_KEY_PASSWORD, trustKS);

    saveConfig(sslClientConfFile, clientSSLConf);
    saveConfig(sslServerConfFile, serverSSLConf);

    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");
    conf.set(SSLFactory.SSL_CLIENT_CONF_KEY, sslClientConfFile.getName());
    conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslServerConfFile.getName());
    conf.setBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, useClientCert);

    return sCert;
  }

  /**
   * Returns an SSL configuration file name.  Under parallel test
   * execution, this file name is parameterized by a unique ID to ensure that
   * concurrent tests don't collide on an SSL configuration file.
   *
   * @param base the base of the file name
   * @return SSL configuration file name for base
   */
  public static String getSSLConfigFileName(String base) {
    String testUniqueForkId = System.getProperty("test.unique.fork.id");
    String fileSuffix = testUniqueForkId != null ? "-" + testUniqueForkId : "";
    return base + fileSuffix + ".xml";
  }

  /**
   * Creates SSL configuration.
   *
   * @param mode SSLFactory.Mode mode to configure
   * @param keystore String keystore file
   * @param keyStorePassword String store password, or null to avoid setting store
   *   password
   * @param keyPassword String key password, or null to avoid setting key
   *   password
   * @param trustKS String truststore file
   * @return Configuration for SSL
   */
  public static Configuration createSSLConfig(SSLFactory.Mode mode,
                                              String keystore, String keyStorePassword,
                                              String keyPassword, String trustKS) {
    String trustPassword = "trustP";

    Configuration sslConf = new Configuration(false);
    if (keystore != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY), keystore);
    }

    if (keyStorePassword != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY), keyStorePassword);
    }
    if (keyPassword != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY),
        keyPassword);
    }
    if (trustKS != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY), trustKS);
    }
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
      FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY),
      trustPassword);
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
      FileBasedKeyStoresFactory.SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY), "1000");

    return sslConf;
  }

  public static KeyPair generateKeyPair(String algorithm)
    throws NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(1024);
    return keyGen.genKeyPair();
  }

  /**
   * Create a self-signed X.509 Certificate.
   *
   * @param dn the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
   * @param pair the KeyPair
   * @param days how many days from now the Certificate is valid for
   * @param algorithm the signing algorithm, eg "SHA1withRSA"
   * @return the self-signed certificate
   */
  public static X509Certificate generateCertificate(String dn, KeyPair pair, int days, String algorithm)
    throws CertificateEncodingException,
    InvalidKeyException,
    IllegalStateException,
    NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86400000l);
    BigInteger sn = new BigInteger(64, new SecureRandom());
    KeyPair keyPair = pair;
    X509V1CertificateGenerator certGen = new X509V1CertificateGenerator();
    X500Principal dnName = new X500Principal(dn);

    certGen.setSerialNumber(sn);
    certGen.setIssuerDN(dnName);
    certGen.setNotBefore(from);
    certGen.setNotAfter(to);
    certGen.setSubjectDN(dnName);
    certGen.setPublicKey(keyPair.getPublic());
    certGen.setSignatureAlgorithm(algorithm);

    X509Certificate cert = certGen.generate(pair.getPrivate());
    return cert;
  }

  public static void createKeyStore(String filename,
                                    String keyPassword, String keyStorePassword,
                                    String alias, Key privateKey, Certificate cert)
    throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    ks.setKeyEntry(alias, privateKey, keyPassword.toCharArray(),
      new Certificate[]{cert});
    saveKeyStore(ks, filename, keyStorePassword);
  }

  public static <T extends Certificate> void createTrustStore(
    String filename, String password, Map<String, T> certs)
    throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    for (Map.Entry<String, T> cert : certs.entrySet()) {
      ks.setCertificateEntry(cert.getKey(), cert.getValue());
    }
    saveKeyStore(ks, filename, password);
  }

  public static void saveKeyStore(KeyStore ks, String filename,
                                   String password)
    throws GeneralSecurityException, IOException {
    FileOutputStream out = new FileOutputStream(filename);
    try {
      ks.store(out, password.toCharArray());
    } finally {
      out.close();
    }
  }

  /**
   * Saves configuration to a file.
   *
   * @param file File to save
   * @param conf Configuration contents to write to file
   * @throws IOException if there is an I/O error saving the file
   */
  public static void saveConfig(File file, Configuration conf)
    throws IOException {
    Writer writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
  }
}
