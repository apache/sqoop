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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.DefaultManagerFactory;
import com.cloudera.sqoop.manager.ManagerFactory;
import com.cloudera.sqoop.metastore.JobData;

import com.cloudera.sqoop.util.ClassLoaderStack;

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
    loadManagersFromConfDir(conf);
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
  public ConnManager getManager(JobData data) throws IOException {
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

  /**
   * Add a ManagerFactory class to the list that we instantiate.
   * @param conf the Configuration to set.
   * @param factory the ManagerFactory class name to add.
   */
  private void addManager(Configuration conf, String factory) {
    String curVal = conf.get(FACTORY_CLASS_NAMES_KEY);
    if (null == curVal) {
      conf.set(FACTORY_CLASS_NAMES_KEY, factory);
    } else {
      conf.set(FACTORY_CLASS_NAMES_KEY, curVal + "," + factory);
    }
  }

  /**
   * Read the specified file and extract any ManagerFactory implementation
   * names from there.
   * @param conf the configuration to populate.
   * @param f the file containing the configuration data to add.
   */
  private void addManagersFromFile(Configuration conf, File f) {
    Reader r = null;
    try {
      // The file format is actually Java properties-file syntax.
      r = new InputStreamReader(new FileInputStream(f));
      Properties props = new Properties();
      props.load(r);

      for (Map.Entry<Object, Object> entry : props.entrySet()) {
        // Each key is a ManagerFactory class name.
        // Each value, if set, is the jar that contains it.
        String factory = entry.getKey().toString();
        addManager(conf, factory);

        String jarName = entry.getValue().toString();
        if (jarName.length() > 0) {
          ClassLoaderStack.addJarFile(jarName, factory);
          LOG.debug("Added factory " + factory + " in jar " + jarName
              + " specified by " + f);
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("Added factory " + factory + " specified by " + f);
        }
      }
    } catch (IOException ioe) {
      LOG.error("Error loading ManagerFactory information from file "
          + f + ": " + StringUtils.stringifyException(ioe));
    } finally {
      if (null != r) {
        try {
          r.close();
        } catch (IOException ioe) {
          LOG.warn("Error closing file " + f + ": " + ioe);
        }
      }
    }
  }

  /**
   * If $SQOOP_CONF_DIR/managers.d/ exists and sqoop.connection.factories is
   * not set, then we look through the files in that directory; they should
   * contain lines of the form mgr.class.name[=/path/to/containing.jar].
   *
   * Put all mgr.class.names into the Configuration, and load any specified
   * jars into the ClassLoader.
   *
   * @param conf the current configuration to populate with class names.
   * @return conf again, after possibly populating sqoop.connection.factories.
   */
  private Configuration loadManagersFromConfDir(Configuration conf) {
    if (conf.get(FACTORY_CLASS_NAMES_KEY) != null) {
      LOG.debug(FACTORY_CLASS_NAMES_KEY + " is set; ignoring managers.d");
      return conf;
    }

    String confDirName = System.getenv("SQOOP_CONF_DIR");
    if (null == confDirName) {
      LOG.warn("$SQOOP_CONF_DIR has not been set in the environment. "
          + "Cannot check for additional configuration.");  
      return conf;
    }

    File confDir = new File(confDirName);
    File mgrDir = new File(confDir, "managers.d");

    if (mgrDir.exists() && mgrDir.isDirectory()) {
      // We have a managers.d subdirectory. Get the file list, sort it,
      // and process them in order.
      String [] fileNames = mgrDir.list();
      Arrays.sort(fileNames);

      for (String fileName : fileNames) {
        File f = new File(mgrDir, fileName);
        if (f.isFile()) {
          addManagersFromFile(conf, f);
        }
      }

      // Add the default MF.
      addManager(conf, DEFAULT_FACTORY_CLASS_NAMES);
    }

    // Set the classloader in this configuration so that it will use
    // the jars we just loaded in.
    conf.setClassLoader(Thread.currentThread().getContextClassLoader());
    return conf;
  }
}

