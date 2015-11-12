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

package org.apache.sqoop.server;

import org.apache.log4j.Logger;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.core.SqoopServer;
import org.apache.sqoop.filter.SqoopAuthenticationFilter;
import org.apache.sqoop.server.v1.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.server.ServerConnector;

import javax.servlet.DispatcherType;
import java.util.EnumSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal is
 * to serve up status information for the server. There are three contexts:
 * "/logs/" -> points to the log directory "/static/" -> points to common static
 * files (src/webapps/static) "/" -> the jsp server code from
 * (src/webapps/<name>)
 */
public class SqoopJettyServer {
  private static final Logger LOG = Logger.getLogger(SqoopJettyServer.class);
  private Server webServer;

  public SqoopJettyServer() {
    SqoopServer.initialize();
    SqoopJettyContext sqoopJettyContext = new SqoopJettyContext(SqoopConfiguration.getInstance().getContext());
    // Server thread pool
    // Start with minWorkerThreads, expand till maxWorkerThreads and reject subsequent requests
    ExecutorService executorService = new ThreadPoolExecutor(sqoopJettyContext.getMinWorkerThreads(),
            sqoopJettyContext.getMaxWorkerThreads(),
            sqoopJettyContext.getWorkerKeepAliveTime(), TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
    ExecutorThreadPool threadPool = new ExecutorThreadPool(executorService);
    webServer = new Server(threadPool);

    // Connector configs
    ServerConnector connector = new ServerConnector(webServer);
    connector.setPort(sqoopJettyContext.getPort());
    webServer.addConnector(connector);
    webServer.setHandler(createServletContextHandler());
  }

  public synchronized void startServer() {
    try {
      webServer.start();
      LOG.info("Started Sqoop Jetty server.");
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException("Sqoop server failed to start.", e);
    }
  }

  public synchronized void joinServerThread() {
    try {
      webServer.join();
    } catch (InterruptedException ie) {
      LOG.info("Sqoop Jetty server is interrupted.");
    }
  }

  // this method is only for test
  public synchronized void stopServerForTest() {
    try {
      if (webServer != null && webServer.isStarted()) {
        webServer.stop();
        SqoopServer.destroy();
        LOG.info("Stopped Sqoop Jetty server.");
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  // this method is only for test
  public String getServerUrl() {
    return webServer.getURI().toString() + "/";
  }

  private static ServletContextHandler createServletContextHandler() {
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/sqoop");
    context.addServlet(AuthorizationServlet.class, "/v1/authorization/*");
    context.addServlet(ConfigurableServlet.class, "/v1/configurable/*");
    context.addServlet(ConnectorServlet.class, "/v1/connector/*");
    context.addServlet(DriverServlet.class, "/v1/driver/*");
    context.addServlet(JobServlet.class, "/v1/job/*");
    context.addServlet(LinkServlet.class, "/v1/link/*");
    context.addServlet(SubmissionsServlet.class, "/v1/submissions/*");
    context.addServlet(VersionServlet.class, "/version");
    context.addFilter(SqoopAuthenticationFilter.class, "/*", EnumSet.allOf(DispatcherType.class));
    return context;
  }

  public static void main(String[] args) {
    SqoopJettyServer sqoopJettyServer = new SqoopJettyServer();
    sqoopJettyServer.startServer();
    sqoopJettyServer.joinServerThread();
  }
}
