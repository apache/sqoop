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

package org.apache.sqoop.manager;

import com.cloudera.sqoop.SqoopOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public enum SupportedManagers {
    MYSQL("jdbc:mysql:", true), POSTGRES("jdbc:postgresql:", true), HSQLDB("jdbc:hsqldb:", false), ORACLE("jdbc:oracle:", true), SQLSERVER("jdbc:sqlserver:", false),
    JTDS_SQLSERVER("jdbc:jtds:sqlserver:", false), DB2("jdbc:db2:", false), NETEZZA("jdbc:netezza:", true), CUBRID("jdbc:cubrid:", false);

    private final String schemePrefix;

    private final boolean hasDirectConnector;
    private static final Log LOG
            = LogFactory.getLog(SupportedManagers.class);

    SupportedManagers(String schemePrefix, boolean hasDirectConnector) {
        this.schemePrefix = schemePrefix;
        this.hasDirectConnector = hasDirectConnector;
    }

    public String getSchemePrefix() {
        return schemePrefix;
    }

    public boolean hasDirectConnector() {
        return hasDirectConnector;
    }

    public boolean isTheManagerTypeOf(SqoopOptions options) {
        return (extractScheme(options)).startsWith(getSchemePrefix());
    }

    public static SupportedManagers createFrom(SqoopOptions options) {
        String scheme = extractScheme(options);
        for (SupportedManagers m : values()) {
            if (scheme.startsWith(m.getSchemePrefix())) {
                return m;
            }
        }
        return null;
    }

    static String extractScheme(SqoopOptions options) {
        String connectStr = options.getConnectString();

        // java.net.URL follows RFC-2396 literally, which does not allow a ':'
        // character in the scheme component (section 3.1). JDBC connect strings,
        // however, commonly have a multi-scheme addressing system. e.g.,
        // jdbc:mysql://...; so we cannot parse the scheme component via URL
        // objects. Instead, attempt to pull out the scheme as best as we can.

        // First, see if this is of the form [scheme://hostname-and-etc..]
        int schemeStopIdx = connectStr.indexOf("//");
        if (-1 == schemeStopIdx) {
            // If no hostname start marker ("//"), then look for the right-most ':'
            // character.
            schemeStopIdx = connectStr.lastIndexOf(':');
            if (-1 == schemeStopIdx) {
                // Warn that this is nonstandard. But we should be as permissive
                // as possible here and let the ConnectionManagers themselves throw
                // out the connect string if it doesn't make sense to them.
                LOG.warn("Could not determine scheme component of connect string");

                // Use the whole string.
                schemeStopIdx = connectStr.length();
            }
        }
        return connectStr.substring(0, schemeStopIdx);
    }
}
