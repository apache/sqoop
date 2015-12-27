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
package org.apache.sqoop.connector.hadoop.security;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.ImmutableContext;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.job.etl.TransferableContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Sqoop is designed in a way to abstract connectors from execution engine. Hence the security portion
 * (like generating and distributing delegation tokens) won't happen automatically for us under the hood
 * and we have to do everything manually.
 */
public class SecurityUtils {

  private static final Logger LOG = Logger.getLogger(SecurityUtils.class);

  private static final String DELEGATION_TOKENS = "org.apache.sqoop.connector.delegation_tokens";

  /**
   * Creates proxy user for user who submitted the Sqoop job (e.g. who has issued the "start job" commnad)
   */
  static public UserGroupInformation createProxyUser(TransferableContext context) throws IOException {
    return UserGroupInformation.createProxyUser(context.getUser(), UserGroupInformation.getLoginUser());
  }

  /**
   * Creates proxy user and load's it up with all delegation tokens that we have created ourselves
   */
  static public UserGroupInformation createProxyUserAndLoadDelegationTokens(TransferableContext context) throws IOException {
    UserGroupInformation proxyUser = createProxyUser(context);
    loadDelegationTokensToUGI(proxyUser, context.getContext());

    return proxyUser;
  }

  /**
   * Generate delegation tokens for current user (this code is suppose to run in doAs) and store them
   * serialized in given mutable context.
   */
  static public void generateDelegationTokens(MutableContext context, Path path, Configuration configuration) throws IOException {
    if(!UserGroupInformation.isSecurityEnabled()) {
      LOG.info("Running on unsecured cluster, skipping delegation token generation.");
      return;
    }

    // String representation of all tokens that we will create (most likely single one)
    List<String> tokens = new LinkedList<>();

    Credentials credentials = new Credentials();
    TokenCache.obtainTokensForNamenodes(credentials, new Path[]{path}, configuration);
    for (Token token : credentials.getAllTokens()) {
      LOG.info("Generated token: " + token.toString());
      tokens.add(serializeToken(token));
    }

    // The context classes are transferred via "Credentials" rather then with jobconf, so we're not leaking the DT out here
    if(tokens.size() > 0) {
      context.setString(DELEGATION_TOKENS, StringUtils.join(tokens, " "));
    }
  }

  /**
   * Loads delegation tokens that we created and serialize into the mutable context
   */
  static public void loadDelegationTokensToUGI(UserGroupInformation ugi, ImmutableContext context) throws IOException {
    String tokenList = context.getString(DELEGATION_TOKENS);
    if(tokenList == null) {
      LOG.info("No delegation tokens found");
      return;
    }

    for(String stringToken: tokenList.split(" ")) {
      Token token = deserializeToken(stringToken);
      LOG.info("Loaded delegation token: " + token.toString());
      ugi.addToken(token);
    }
  }

  /**
   * Serialize given token into String.
   *
   * We'll convert token to byte[] using Writable methods fro I/O and then Base64
   * encode the bytes to a human readable string.
   */
  static public String serializeToken(Token token) throws IOException {
    // Serialize the Token to a byte array
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    token.write(dos);
    baos.flush();

    return Base64.encodeBase64String(baos.toByteArray());
  }

  /**
   * Deserialize token from given String.
   *
   * See serializeToken for details how the token is expected to be serialized.
   */
  static public Token deserializeToken(String stringToken) throws IOException {
    Token token = new Token();
    byte[] tokenBytes = Base64.decodeBase64(stringToken);

    ByteArrayInputStream bais = new ByteArrayInputStream(tokenBytes);
    DataInputStream dis = new DataInputStream(bais);
    token.readFields(dis);

    return token;
  }

  private SecurityUtils() {
    // Initialization is prohibited
  }
}
