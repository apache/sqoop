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
package org.apache.sqoop.test.infrastructure;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.InfrastructureProvider;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.ITest;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Use Infrastructure annotation to boot up miniclusters.
 * Order is built-in to code. Hadoop comes first, then
 * the rest of the services.
 */
public class SqoopTestCase implements ITest {
  private static final Logger LOG = Logger.getLogger(SqoopTestCase.class);

  private static final String ROOT_PATH = System.getProperty("sqoop.integration.tmpdir", System.getProperty("java.io.tmpdir", "/tmp")) + "/sqoop-cargo-tests";

  private static final Map<String, InfrastructureProvider> PROVIDERS
      = new HashMap<String, InfrastructureProvider>();

  private static String suiteName;

  private String methodName;

  @BeforeSuite
  public static void findSuiteName(ITestContext context) {
    suiteName = context.getSuite().getName();
  }

  @BeforeMethod
  public void findMethodName(Method method) {
    methodName = method.getName();
  }

  @Override
  public String getTestName() {
    return methodName;
  }

  /**
   * Create infrastructure components and start those services.
   * @param context TestNG context that helps get all the test methods and classes.
   */
  @BeforeSuite(dependsOnMethods = "findSuiteName")
  public static void startInfrastructureProviders(ITestContext context) {
    // Find infrastructure provider classes to be used.
    Set<Class<? extends InfrastructureProvider>> providers = new HashSet<Class<? extends InfrastructureProvider>>();
    for (ITestNGMethod method : context.getSuite().getAllMethods()) {
      Infrastructure ann;

      // If the method has an infrastructure annotation, process it.
      if (method.getConstructorOrMethod().getMethod() != null) {
        ann = method.getConstructorOrMethod().getMethod().getAnnotation(Infrastructure.class);
        if (ann != null && ann.dependencies() != null) {
          LOG.debug("Found dependencies on method ("
              + method.getConstructorOrMethod().getDeclaringClass().getCanonicalName()
              + "#" + method.getConstructorOrMethod().getMethod().getName()
              + "): " + StringUtils.join(ann.dependencies(), ","));
          providers.addAll(Arrays.asList(ann.dependencies()));
        }
      }

      // Declaring class should be processed always.
      ann = method.getConstructorOrMethod().getDeclaringClass().getAnnotation(Infrastructure.class);
      if (ann != null && ann.dependencies() != null) {
        LOG.debug("Found dependencies on class ("
            + method.getConstructorOrMethod().getDeclaringClass().getCanonicalName()
            + "): " + StringUtils.join(ann.dependencies(), ","));
        providers.addAll(Arrays.asList(ann.dependencies()));
      }
    }

    // Create/start infrastructure providers.
    Configuration conf = new JobConf();

    // Start hadoop first.
    if (providers.contains(HadoopInfrastructureProvider.class)) {
      InfrastructureProvider hadoopProviderObject = startInfrastructureProvider(HadoopInfrastructureProvider.class, conf);

      // Use the prepared hadoop configuration for the rest of the components.
      if (hadoopProviderObject != null) {
        conf = hadoopProviderObject.getHadoopConfiguration();
      }
    }

    // Start the rest of the providers.
    for (Class<? extends InfrastructureProvider> provider : providers) {
      startInfrastructureProvider(provider, conf);
    }
  }

  /**
   * Start an infrastructure provider and add it to the PROVIDERS map
   * for stopping in the future.
   * @param providerClass
   * @param hadoopConfiguration
   * @param <T>
   * @return
   */
  private static <T extends InfrastructureProvider> T startInfrastructureProvider(Class<T> providerClass, Configuration hadoopConfiguration) {
    T providerObject;

    try {
      providerObject = providerClass.newInstance();
    } catch (Exception e) {
      LOG.error("Could not instantiate new instance of InfrastructureProvider.", e);
      return null;
    }

    providerObject.setRootPath(HdfsUtils.joinPathFragments(ROOT_PATH, suiteName, providerClass.getCanonicalName()));
    providerObject.setHadoopConfiguration(hadoopConfiguration);
    providerObject.start();

    // Add for recall later.
    PROVIDERS.put(providerClass.getCanonicalName(), providerObject);

    System.out.println("Infrastructure Provider " + providerClass.getCanonicalName());

    return providerObject;
  }

  /**
   * Stop infrastructure components and services.
   */
  @AfterSuite
  public static void stopInfrastructureProviders() {
    // Hadoop infrastructure provider included in PROVIDERS.
    for (InfrastructureProvider provider : PROVIDERS.values()) {
      provider.stop();
    }
  }

  /**
   * Get the infrastructure provider from the PROVIDERS map.
   * @param providerClass
   * @param <T>
   * @return T InfrastructureProvider
   */
  public static <T extends InfrastructureProvider> T getInfrastructureProvider(Class<T> providerClass) {
    InfrastructureProvider provider = PROVIDERS.get(providerClass.getCanonicalName());
    return ((T) provider);
  }
}
