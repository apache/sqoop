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

package org.apache.sqoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
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
import org.apache.sqoop.manager.GenericJdbcManager;
import org.apache.sqoop.manager.oracle.OraOopManagerFactory;

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
  public static final String[] DEFAULT_FACTORY_CLASS_NAMES_ARR =
      {OraOopManagerFactory.class.getName(),
      DefaultManagerFactory.class.getName(), };

  public static final String DEFAULT_FACTORY_CLASS_NAMES =
      StringUtils.arrayToString(DEFAULT_FACTORY_CLASS_NAMES_ARR);

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
        conf.getStrings(FACTORY_CLASS_NAMES_KEY,
            DEFAULT_FACTORY_CLASS_NAMES_ARR);

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
   * Factory method to get a ConnManager.
   *
   * Connection Manager is created directly if user specifies it on the command
   * line or the execution is passed to various configured connection factories
   * in case that user is not requesting one specific manager.
   *
   * @param data the connection and other configuration arguments.
   * @return a ConnManager instance for the appropriate database.
   * @throws IOException if it cannot find a ConnManager for this schema.
   */
  public ConnManager getManager(JobData data) throws IOException {
    com.cloudera.sqoop.SqoopOptions options = data.getSqoopOptions();
    String manualDriver = options.getDriverClassName();
    String managerClassName = options.getConnManagerClassName();

    // User has specified --driver argument, but he did not specified
    // manager to use. We will use GenericJdbcManager as this was
    // the way sqoop was working originally. However we will inform
    // user that specifying connection manager explicitly is more cleaner
    // solution for this case.
    if (manualDriver != null && managerClassName == null) {
      LOG.warn("Parameter --driver is set to an explicit driver however"
        + " appropriate connection manager is not being set (via"
        + " --connection-manager). Sqoop is going to fall back to "
        + GenericJdbcManager.class.getCanonicalName() + ". Please specify"
        + " explicitly which connection manager should be used next time."
      );
      return new GenericJdbcManager(manualDriver, options);
    }

    // If user specified explicit connection manager, let's use it
    if (managerClassName != null){
      ConnManager connManager = null;

      try {
        Class<ConnManager> cls = (Class<ConnManager>)
          Class.forName(managerClassName);

        // We have two constructor options, one is with or without explicit
        // constructor. In most cases --driver argument won't be allowed as the
        // connectors are forcing to use their building class names.
        if (manualDriver == null) {
          Constructor<ConnManager> constructor =
            cls.getDeclaredConstructor(com.cloudera.sqoop.SqoopOptions.class);
          connManager = constructor.newInstance(options);
        } else {
          Constructor<ConnManager> constructor =
            cls.getDeclaredConstructor(String.class,
                                       com.cloudera.sqoop.SqoopOptions.class);
          connManager = constructor.newInstance(manualDriver, options);
        }
      } catch (ClassNotFoundException e) {
        LOG.error("Sqoop could not found specified connection manager class "
          + managerClassName  + ". Please check that you've specified the "
          + "class correctly.");
        throw new IOException(e);
      } catch (NoSuchMethodException e) {
        LOG.error("Sqoop wasn't able to create connnection manager properly. "
          + "Some of the connectors supports explicit --driver and some "
          + "do not. Please try to either specify --driver or leave it out.");
        throw new IOException(e);
      } catch (Exception e) {
        LOG.error("Problem with bootstrapping connector manager:"
          + managerClassName);
        LOG.error(e);
        throw new IOException(e);
      }
      return connManager;
    }

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
    BufferedReader r = null;
    try {
      // The file format is actually Java properties-file syntax.
      r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
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
   * <p>
   * Put all mgr.class.names into the Configuration, and load any specified
   * jars into the ClassLoader.
   * </p>
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
      String[] fileNames;

      try {
        fileNames = mgrDir.list();
      } catch (SecurityException e) {
        fileNames = null;
      }

      if (null == fileNames) {
        LOG.warn("Sqoop cannot read $SQOOP_CONF_DIR/managers.d. "
            + "Please check the permissions on managers.d.");
        return conf;
      }

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

