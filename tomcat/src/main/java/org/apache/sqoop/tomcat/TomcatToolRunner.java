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
package org.apache.sqoop.tomcat;

import org.apache.catalina.Host;
import org.apache.catalina.startup.Bootstrap;
import org.apache.catalina.startup.ClassLoaderFactory;
import org.apache.catalina.startup.Embedded;
import org.apache.catalina.startup.ExpandWar;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;

/**
 * Add Sqoop webapp and common loader into the classpath and run the usual ToolRunner.
 *
 * This class will be executed via Tomcat Tool mechanism that do a lot of heavy
 * lifting for us - it will set up most of the environment and class loaders. Sadly
 * it won't setup the common.loader (Hadoop dependencies) and the Sqoop webapp itself.
 */
public class TomcatToolRunner {

  // TODO: The appBase can be loaded from the conf/server.xml file
  private static String PROPERTY_APPBASE_PATH = "org.apache.sqoop.tomcat.webapp.path";
  private static String DEFAULT_APPBASE_PATH = "webapps";

  public static void main(String[] args) throws Exception {
    // Using Boostrap class to boot the common.loader and other catalina specific
    // class loaders.
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.init();

    // Now we need to add the sqoop webapp classes into the class loader. Sadly
    // we have to do a lot of things ourselves. The procedure is:
    // 1) Unpack Sqoop war file
    // 2) Build the ClassLoader using Tomcat's ClassLoaderFactory

    // Various paths to war file and locations inside the war file
    String webappPath = System.getProperty(PROPERTY_APPBASE_PATH, DEFAULT_APPBASE_PATH);
    String catalinaBase = Bootstrap.getCatalinaBase();
    String fullWebappPath = catalinaBase + File.separator + webappPath;
    String fullSqoopWarPath = fullWebappPath + File.separator + "sqoop.war";
    String fullSqoopClassesPath = fullWebappPath + File.separator + "sqoop" + File.separator + "WEB-INF" + File.separator + "classes";
    String fullSqoopLibPath = fullWebappPath + File.separator + "sqoop" + File.separator + "WEB-INF" + File.separator + "lib";

    // Expand the war into the usual location, this operation is idempotent (nothing bad happens if it's already expanded)
    Embedded embedded = new Embedded();
    Host host = embedded.createHost("Sqoop Tool Virtual Host", fullWebappPath);
    ExpandWar.expand(host, new URL("jar:file://" + fullSqoopWarPath + "!/"));

    // We have expanded war file, so we build the classloader from
    File [] unpacked = new File[1];   unpacked[0] = new File(fullSqoopClassesPath);
    File [] packed = new File[1];     packed[0] = new File(fullSqoopLibPath);
    ClassLoader loader = ClassLoaderFactory.createClassLoader(unpacked, packed, Thread.currentThread().getContextClassLoader());
    Thread.currentThread().setContextClassLoader(loader);

    // Finally we can call the usual ToolRunner. We have to use reflection as
    // as the time of loading this class, Sqoop dependencies are not on classpath.
    Class klass = Class.forName("org.apache.sqoop.tools.ToolRunner", true, loader);
    Method method = klass.getMethod("main", String[].class);
    method.invoke(null, (Object)args);
  }

}
