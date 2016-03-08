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
package org.apache.sqoop.shell;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.utils.ProcessUtils;
import org.apache.sqoop.validation.Status;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import static org.apache.sqoop.shell.ShellEnvironment.printlnResource;
import static org.apache.sqoop.shell.ShellEnvironment.resourceString;

@SuppressWarnings("serial")
public class SetTruststoreFunction extends SqoopFunction {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("static-access")
  public SetTruststoreFunction() {
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_TRUSTSTORE)
        .withDescription(resourceString(Constants.RES_TRUSTSTORE_DESCRIPTION))
        .withLongOpt(Constants.OPT_TRUSTSTORE)
        .create());
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_TRUSTSTORE_PASSWORD)
        .withDescription(resourceString(Constants.RES_TRUSTSTORE_PASSWORD_DESCRIPTION))
        .withLongOpt(Constants.OPT_TRUSTSTORE_PASSWORD)
        .create());
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_TRUSTSTORE_PASSWORD_GENERATOR)
        .withDescription(resourceString(Constants.RES_TRUSTSTORE_PASSWORD_GENERATOR_DESCRIPTION))
        .withLongOpt(Constants.OPT_TRUSTSTORE_PASSWORD_GENERATOR)
        .create());
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) throws IOException {
    try {
      if (line.hasOption(Constants.OPT_TRUSTSTORE)) {
        String truststoreLocation = line.getOptionValue(Constants.OPT_TRUSTSTORE);

        char[] truststorePassword = null;
        if (line.hasOption(Constants.OPT_TRUSTSTORE_PASSWORD)) {
          truststorePassword = line.getOptionValue(Constants.OPT_TRUSTSTORE_PASSWORD).toCharArray();
        } else if (line.hasOption(Constants.OPT_TRUSTSTORE_PASSWORD_GENERATOR)) {
          String generator = line.getOptionValue(Constants.OPT_TRUSTSTORE_PASSWORD_GENERATOR);
          truststorePassword = ProcessUtils.readOutputFromGenerator(generator).toCharArray();
        }

        KeyStore keyStore = KeyStore.getInstance("JKS");

        File truststore = new File(truststoreLocation);
        try (FileInputStream trustStoreFileInputStream = new FileInputStream(truststore)) {
          keyStore.load(trustStoreFileInputStream, truststorePassword);
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trustManagerFactory.getTrustManagers(), null);
        SSLSocketFactory sslSocketFactory = context.getSocketFactory();
        HttpsURLConnection.setDefaultSSLSocketFactory(sslSocketFactory);
      }
    } catch (IOException|CertificateException|NoSuchAlgorithmException|KeyStoreException|KeyManagementException exception) {
      printlnResource(Constants.RES_SET_TRUSTSTORE_FAILED);
      throw new IOException("failed to set truststore", exception);
    }

    printlnResource(Constants.RES_SET_TRUSTSTORE_SUCCESSFUL);
    return Status.OK;
  }
}
