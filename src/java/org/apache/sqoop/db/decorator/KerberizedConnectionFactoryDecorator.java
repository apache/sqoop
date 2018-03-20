/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.db.decorator;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sqoop.authentication.KerberosAuthenticator;
import org.apache.sqoop.db.JdbcConnectionFactory;

import java.security.PrivilegedAction;
import java.sql.Connection;

public class KerberizedConnectionFactoryDecorator extends JdbcConnectionFactoryDecorator {

  private final KerberosAuthenticator authenticator;

  public KerberizedConnectionFactoryDecorator(JdbcConnectionFactory decorated, KerberosAuthenticator authenticator) {
    super(decorated);
    this.authenticator = authenticator;
  }

  @Override
  public Connection createConnection() {
    UserGroupInformation ugi = authenticator.authenticate();
    return ugi.doAs(new PrivilegedAction<Connection>() {
      @Override
      public Connection run() {
        return decorated.createConnection();
      }
    });
  }

  public KerberosAuthenticator getAuthenticator() {
    return authenticator;
  }
}
