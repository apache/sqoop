/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.DefaultManagerFactory;
import com.cloudera.sqoop.manager.ManagerFactory;
import com.cloudera.sqoop.metastore.SessionData;

/**
 * Factory class to create the ConnManager type required
 * for the current import job.
 *
 * This class delegates the actual responsibility for instantiating
 * ConnManagers to one or more instances of ManagerFactory. ManagerFactories
 * are consulted in the order specified in sqoop-site.xml
 * (sqoop.connection.factories).
 */
public class ConnFactory {

  public static final Log LOG = LogFactory.getLog(ConnFactory.class.getName());

  public ConnFactory(Configuration conf) {
    factories = new LinkedList<ManagerFactory>();
    instantiateFactories(conf);
  }

  /** The sqoop-site.xml configuration property used to set the list of 
   * available ManagerFactories.
   */
  public static final String FACTORY_CLASS_NAMES_KEY =
      "sqoop.connection.factories";

  // The default value for sqoop.connection.factories is the
  // name of the DefaultManagerFactory.
  static final String DEFAULT_FACTORY_CLASS_NAMES =
      DefaultManagerFactory.class.getName(); 

  /** The list of ManagerFactory instances consulted by getManager().
   */
  private List<ManagerFactory> factories;

  /**
   * Create the ManagerFactory instances that should populate
   * the factories list.
   */
  private void instantiateFactories(Configuration conf) {
    String [] classNameArray =
        conf.getStrings(FACTORY_CLASS_NAMES_KEY, DEFAULT_FACTORY_CLASS_NAMES);

    for (String className : classNameArray) {
      try {
        className = className.trim(); // Ignore leading/trailing whitespace.
        ManagerFactory factory = ReflectionUtils.newInstance(
            (Class<? extends ManagerFactory>)
            conf.getClassByName(className), conf);
        LOG.debug("Loaded manager factory: " + className);
        factories.add(factory);
      } catch (ClassNotFoundException cnfe) {
        LOG.error("Could not load ManagerFactory " + className
            + " (not found)");
      }
    }
  }

  /**
   * Factory method to get a ConnManager for the given JDBC connect string.
   * @param data the connection and other configuration arguments.
   * @return a ConnManager instance for the appropriate database.
   * @throws IOException if it cannot find a ConnManager for this schema.
   */
  public ConnManager getManager(SessionData data) throws IOException {
    // Try all the available manager factories.
    for (ManagerFactory factory : factories) {
      LOG.debug("Trying ManagerFactory: " + factory.getClass().getName());
      ConnManager mgr = factory.accept(data);
      if (null != mgr) {
        LOG.debug("Instantiated ConnManager " + mgr.toString());
        return mgr;
      }
    }

    throw new IOException("No manager for connect string: "
        + data.getSqoopOptions().getConnectString());
  }
}

