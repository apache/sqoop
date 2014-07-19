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

package org.apache.sqoop.manager.oracle;

import org.apache.sqoop.manager.oracle.OraOopUtilities.JdbcOracleThinConnection;
import org.apache.sqoop.manager.oracle.OraOopUtilities
         .JdbcOracleThinConnectionParsingError;

/**
 * Parses the Oracle connection string.
 */
public class OraOopJdbcUrl {

  private String jdbcConnectString;

  public OraOopJdbcUrl(String jdbcConnectString) {

    if (jdbcConnectString == null) {
      throw new IllegalArgumentException(
          "The jdbcConnectionString argument must not be null.");
    }

    if (jdbcConnectString.isEmpty()) {
      throw new IllegalArgumentException(
          "The jdbcConnectionString argument must not be empty.");
    }

    this.jdbcConnectString = jdbcConnectString;
  }

  public JdbcOracleThinConnection parseJdbcOracleThinConnectionString()
      throws JdbcOracleThinConnectionParsingError {

    /*
     * http://wiki.oracle.com/page/JDBC
     *
     * There are different flavours of JDBC connections for Oracle, including:
     * Thin E.g. jdbc:oracle:thin:@localhost.locadomain:1521:orcl
     *
     * A pure Java driver used on the client side that does not need an Oracle
     * client installation. It is recommended that you use this driver unless
     * you need support for non-TCP/IP networks because it provides for maximum
     * portability and performance.
     *
     * Oracle Call Interface driver (OCI). E.g. jdbc:oracle:oci8:@orcl.world
     * //<- "orcl.world" is a TNS entry
     *
     * This uses the Oracle client installation libraries and interfaces. If you
     * want to support connection pooling or client side caching of requests,
     * use this driver. You will also need this driver if you are using
     * transparent application failover (TAF) from your application as well as
     * strong authentication like Kerberos and PKI certificates.
     *
     * JDBC-ODBC bridge. E.g. jdbc:odbc:mydatabase //<- "mydatabase" is an ODBC
     * data source.
     *
     * This uses the ODBC driver in Windows to connect to the database.
     */

    String hostName = null;
    int port = 0;
    String sid = null;
    String service = null;

    String jdbcUrl = this.jdbcConnectString.trim();

    // If there are any parameters included at the end of the connection URL,
    // let's remove them now...
    int paramsIdx = jdbcUrl.indexOf("?");
    if (paramsIdx > -1) {
      jdbcUrl = jdbcUrl.substring(0, paramsIdx);
    }

    /*
     * The format of an Oracle jdbc URL is one of:
     * jdbc:oracle:<driver-type>:@tnsname - for tnsname based login
     * jdbc:oracle:<driver-type>:@<host>:<port>:<sid>
     * jdbc:oracle:<driver-type>:@<host>:<port>/<service>
     * jdbc:oracle:<driver-type>:@<host>:<port>/<service>?<parameters>
     * jdbc:oracle:<driver-type>:@//<host>:<port>/<service>
     * jdbc:oracle:<driver-type>:@//<host>:<port>/<service>?<parameters>
     */

    // Split the URL on its ":" characters...
    String[] jdbcFragments = jdbcUrl.trim().split(":");

    // Clean up each fragment of the URL...
    for (int idx = 0; idx < jdbcFragments.length; idx++) {
      jdbcFragments[idx] = jdbcFragments[idx].trim();
    }

    // Check we can proceed...
    if (jdbcFragments.length < 4 || jdbcFragments.length > 6) {
      throw new JdbcOracleThinConnectionParsingError(
        String.format(
          "There should be 4, 5 or 6 colon-separated pieces of data in the "
        + "JDBC URL, such as:\n\tjdbc:oracle:<driver-type>:@tnsname\n"
        + "\tjdbc:oracle:<driver-type>:@<host>:<port>:<sid>\n"
        + "\tjdbc:oracle:<driver-type>:@<host>:<port>/<service>\n"
        + "\tjdbc:oracle:<driver-type>:@<host>:<port>/<service>?<parameters>\n"
        + "The JDBC URL specified was:\n"
        + "%s\n"
        + "which contains %d pieces of colon-separated data.",
                  this.jdbcConnectString, jdbcFragments.length));
    }

    // jdbc
    if (!jdbcFragments[0].equalsIgnoreCase("jdbc")) {
      throw new JdbcOracleThinConnectionParsingError(
          "The first item in the colon-separated JDBC URL must be \"jdbc\".");
    }

    // jdbc:oracle
    if (!jdbcFragments[1].equalsIgnoreCase("oracle")) {
      throw new JdbcOracleThinConnectionParsingError(
        "The second item in the colon-separated JDBC URL must be \"oracle\".");
    }

    // jdbc:oracle:thin
    if (!jdbcFragments[2].equalsIgnoreCase("thin")) {
      throw new JdbcOracleThinConnectionParsingError(
          String
              .format(
                  "The Oracle \"thin\" JDBC driver is not being used.\n"
                      + "The third item in the colon-separated JDBC URL must "
                      + "be \"thin\", not \"%s\".",
                  jdbcFragments[2]));
    }

    // jdbc:oracle:thin:@<host>
    hostName = jdbcFragments[3];
    if (hostName.isEmpty() || hostName.equalsIgnoreCase("@")) {
      throw new JdbcOracleThinConnectionParsingError(
          "The fourth item in the colon-separated JDBC URL (the host name) "
          + "must not be empty.");
    }

    if (!hostName.startsWith("@")) {
      throw new JdbcOracleThinConnectionParsingError(
          "The fourth item in the colon-separated JDBC URL (the host name) "
          + "must a prefixed with the \"@\" character.");
    }

    String portStr = "";
    String tnsName = "";

    switch (jdbcFragments.length) {
      case 6:
        // jdbc:oracle:<driver-type>:@<host>:<port>:<sid>
        portStr = jdbcFragments[4];
        sid = jdbcFragments[5];
        break;

      case 5:
        // jdbc:oracle:<driver-type>:@<host>:<port>/<service>
        String[] portAndService = jdbcFragments[4].split("/");
        if (portAndService.length != 2) {
          throw new JdbcOracleThinConnectionParsingError(
              "The fifth colon-separated item in the JDBC URL "
              + "(<port>/<service>) must contain two items "
              + "separated by a \"/\".");
        }
        portStr = portAndService[0].trim();
        service = portAndService[1].trim();
        break;

      case 4:
        // jdbc:oracle:<driver-type>:@tnsname
        tnsName = jdbcFragments[3].trim();
        break;

      default:
        throw new JdbcOracleThinConnectionParsingError("Internal error parsing "
            + "JDBC connection string.");
    }

    if (jdbcFragments.length > 4) {
      if (portStr.isEmpty()) {
        throw new JdbcOracleThinConnectionParsingError(
            "The fifth item in the colon-separated JDBC URL (the port) must not"
            + " be empty.");
      }

      try {
        port = Integer.parseInt(portStr);
      } catch (NumberFormatException ex) {
        throw new JdbcOracleThinConnectionParsingError(
            String
                .format(
                    "The fifth item in the colon-separated JDBC URL (the port) "
                    + "must be a valid number.\n"
                    + "\"%s\" could not be parsed as an integer.", portStr));
      }

      if (port <= 0) {
        throw new JdbcOracleThinConnectionParsingError(
            String
                .format(
                    "The fifth item in the colon-separated JDBC URL (the port) "
                    + "must be greater than zero.\n"
                        + "\"%s\" was specified.", portStr));
      }
    }

    if (sid == null && service == null && tnsName == null) {
      throw new JdbcOracleThinConnectionParsingError(
        "The JDBC URL does not contain a SID or SERVICE. The URL should look "
      + "like one of these:\n\tjdbc:oracle:<driver-type>:@tnsname\n"
      + "\tjdbc:oracle:<driver-type>:@<host>:<port>:<sid>\n"
      + "\tjdbc:oracle:<driver-type>:@<host>:<port>/<service>\n"
      + "\tjdbc:oracle:<driver-type>:@<host>:<port>/<service>?<parameters>\n"
      + "\tjdbc:oracle:<driver-type>:@//<host>:<port>/<service>\n"
      + "\tjdbc:oracle:<driver-type>:@<host>:<port>/<service>?<parameters>");
    }

    // Remove the "@" prefix of the hostname
    JdbcOracleThinConnection result =
        new JdbcOracleThinConnection(hostName.replaceFirst("^[@][/]{0,2}", "")
            , port, sid, service, tnsName.replaceFirst("^[@][/]{0,2}", ""));

    return result;
  }

  public String getConnectionUrl() {
    return this.jdbcConnectString;
  }

}
