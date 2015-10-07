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

package org.apache.sqoop.integration.classpath;

import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.minicluster.JettySqoopMiniCluster;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class ClasspathTest extends ConnectorTestCase {

  private static final String TEST_CONNECTOR_JAR_NAME = "test-connector.jar";
  private static final String TEST_DEPENDENCY_JAR_NAME = "test-dependency.jar";

  private ClassLoader classLoader;

  public static class DerbySqoopMiniCluster extends JettySqoopMiniCluster {

    private String extraClasspath;
    private String jobExtraClasspath;

    public DerbySqoopMiniCluster(String temporaryPath, Configuration configuration, String extraClasspath, String jobExtraClasspath) throws Exception {
      super(temporaryPath, configuration);
      this.extraClasspath = extraClasspath;
      this.jobExtraClasspath = jobExtraClasspath;
    }

    @Override
    protected Map<String, String> getClasspathConfiguration() {
      Map<String, String> properties = new HashMap<>();

      if (extraClasspath != null) {
        properties.put(ConfigurationConstants.CLASSPATH, extraClasspath);
      }
      if (jobExtraClasspath != null) {
        properties.put(ConfigurationConstants.JOB_CLASSPATH, jobExtraClasspath);
      }


      return properties;
    }
  }

  class JarContents {
    private List<File> sourceFiles;
    private List<File> properitesFiles;

    public JarContents(List<File> sourceFiles, List<File> properitesFiles){
      this.sourceFiles = sourceFiles;
      this.properitesFiles = properitesFiles;
    }

    public List<File> getSourceFiles() {
      return sourceFiles;
    }

    public List<File> getProperitesFiles() {
      return properitesFiles;
    }
  }

  public void startSqoopMiniCluster(String extraClasspath, String jobExtraClasspath) throws Exception {
    // And use them for new Derby repo instance
    setCluster(new DerbySqoopMiniCluster(HdfsUtils.joinPathFragments(super.getSqoopMiniClusterTemporaryPath(), getTestName()), hadoopCluster.getConfiguration(), extraClasspath, jobExtraClasspath));

    // Start server
    getCluster().start();

    // Initialize Sqoop Client API
    setClient(new SqoopClient(getServerUrl()));
  }

  @BeforeMethod
  public void captureClasspath() {
    classLoader = Thread.currentThread().getContextClassLoader();
  }

  @AfterMethod
  public void restoreClasspath(){
    Thread.currentThread().setContextClassLoader(classLoader);
  }

  @Test
  public void testClasspathSqoopProperties() throws Exception {
    Map<String, String> jarMap = compileTestConnectorAndDependency();
    startSqoopMiniCluster(jarMap.get(TEST_CONNECTOR_JAR_NAME), jarMap.get
      (TEST_DEPENDENCY_JAR_NAME));
    createAndLoadTableCities();

    MJob job = prepareJob();

    prepareDriverConfig(job);

    saveJob(job);

    executeJob(job);

    stopSqoop();
    deleteJars(jarMap);
  }

  @Test
  public void testClasspathDriverInput() throws Exception{
    Map<String, String> jarMap = compileTestConnectorAndDependency();
    startSqoopMiniCluster(jarMap.get(TEST_CONNECTOR_JAR_NAME), null);
    createAndLoadTableCities();

    MJob job = prepareJob();

    MDriverConfig driverConfig = prepareDriverConfig(job);

    List<String> extraJars = new ArrayList<>();
    extraJars.add("file:" + jarMap.get(TEST_DEPENDENCY_JAR_NAME));
    driverConfig.getListInput("jarConfig.extraJars").setValue(extraJars);

    saveJob(job);

    executeJob(job);

    stopSqoop();
    deleteJars(jarMap);
  }

  private MJob prepareJob() {
    MLink rdbmsConnection = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsConnection);
    saveLink(rdbmsConnection);

    MLink testConnection = getClient().createLink("test-connector");
    saveLink(testConnection);

    MJob job = getClient().createJob(rdbmsConnection.getPersistenceId(), testConnection.getPersistenceId());

    fillRdbmsFromConfig(job, "id");

    return job;
  }

  private MDriverConfig prepareDriverConfig(MJob job) {
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);

    return driverConfig;
  }

  private Map<String, String> compileTestConnectorAndDependency() throws Exception {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new IllegalStateException(
        "Cannot find the system Java compiler. "
          + "Check that your class path includes tools.jar");
    }

    Path outputDir = Files.createTempDirectory(null);

    Map<String, JarContents> sourceFileToJarMap = new HashMap<>();

    ClassLoader classLoader = getClass().getClassLoader();
    List<File> sourceFiles = new ArrayList<>();
    File file = new File(classLoader.getResource("TestConnector/TestConnector.java").getFile());
    sourceFiles.add(file);
    file = new File(classLoader.getResource("TestConnector/TestLinkConfiguration.java").getFile());
    sourceFiles.add(file);
    file = new File(classLoader.getResource("TestConnector/TestLoader.java").getFile());
    sourceFiles.add(file);
    file = new File(classLoader.getResource("TestConnector/TestToDestroyer.java").getFile());
    sourceFiles.add(file);
    file = new File(classLoader.getResource("TestConnector/TestToInitializer.java").getFile());
    sourceFiles.add(file);
    file = new File(classLoader.getResource("TestConnector/TestToJobConfiguration.java").getFile());
    sourceFiles.add(file);

    List<File> propertiesFiles = new ArrayList<>();
    file = new File(classLoader.getResource("TestConnector/sqoopconnector.properties").getFile());
    propertiesFiles.add(file);
    sourceFileToJarMap.put("test-connector.jar", new JarContents(sourceFiles, propertiesFiles));

    sourceFiles = new ArrayList<>();
    file = new File(classLoader.getResource("TestConnector/TestDependency.java").getFile());
    sourceFiles.add(file);
    sourceFileToJarMap.put("test-dependency.jar", new JarContents(sourceFiles, ListUtils.EMPTY_LIST));

    return buildJar(outputDir.toString(), sourceFileToJarMap);
  }

  private Map<String, String> buildJar(String outputDir, Map<String, JarContents> sourceFileToJarMap) throws Exception {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager = compiler.getStandardFileManager
      (null, null, null);

    List<File> sourceFiles = new ArrayList<>();
    for(JarContents jarContents : sourceFileToJarMap.values()) {
      sourceFiles.addAll(jarContents.sourceFiles);
    }

    fileManager.setLocation(StandardLocation.CLASS_OUTPUT,
      Arrays.asList(new File(outputDir.toString())));

    Iterable<? extends JavaFileObject> compilationUnits1 =
      fileManager.getJavaFileObjectsFromFiles(sourceFiles);

    boolean compiled = compiler.getTask(null, fileManager, null, null, null, compilationUnits1).call();
    if (!compiled) {
      throw new RuntimeException("failed to compile");
    }

    for(Map.Entry<String, JarContents> jarNameAndContents : sourceFileToJarMap.entrySet()) {
      Manifest manifest = new Manifest();
      manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
      manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, ".");


      JarOutputStream target = new JarOutputStream(new FileOutputStream(outputDir.toString() + File.separator + jarNameAndContents.getKey()), manifest);
      List<String> classesForJar = new ArrayList<>();
      for(File sourceFile : jarNameAndContents.getValue().getSourceFiles()) {
        //split the file on dot to get the filename from FILENAME.java
        String fileName = sourceFile.getName().split("\\.")[0];
        classesForJar.add(fileName);
      }

      File dir = new File(outputDir);
      File[] directoryListing = dir.listFiles();
      for (File compiledClass : directoryListing) {
        String classFileName = compiledClass.getName().split("\\$")[0].split("\\.")[0];
        if (classesForJar.contains(classFileName)){
          addFileToJar(compiledClass, target);
        }
      }

      for (File propertiesFile : jarNameAndContents.getValue().getProperitesFiles()) {
        addFileToJar(propertiesFile, target);
      }

      target.close();


    }
    //delete non jar files
    File dir = new File(outputDir);
    File[] directoryListing = dir.listFiles();
    for (File file : directoryListing) {
      String extension = file.getName().split("\\.")[1];
      if (!extension.equals("jar")) {
        file.delete();
      }
    }

    Map<String, String> jarMap = new HashMap<>();
    jarMap.put(TEST_CONNECTOR_JAR_NAME, outputDir.toString() + File.separator
      + TEST_CONNECTOR_JAR_NAME);
    jarMap.put(TEST_DEPENDENCY_JAR_NAME, outputDir.toString() + File.separator + TEST_DEPENDENCY_JAR_NAME);
    return jarMap;
  }

  private void addFileToJar(File source, JarOutputStream target) throws Exception {
    JarEntry entry = new JarEntry(source.getName());
    entry.setTime(source.lastModified());
    target.putNextEntry(entry);
    BufferedInputStream in = new BufferedInputStream(new FileInputStream(source));

    long bufferSize = source.length();
    if (bufferSize < Integer.MIN_VALUE || bufferSize > Integer.MAX_VALUE) {
      throw new RuntimeException("file to large to be added to jar");
    }

    byte[] buffer = new byte[(int) bufferSize];
    while (true) {
      int count = in.read(buffer);
      if (count == -1)
        break;
      target.write(buffer, 0, count);
    }
    target.closeEntry();
    if (in != null) in.close();
  }

  private void deleteJars(Map<String, String> jarMap) throws Exception {
    for (String jarPath : jarMap.values()) {
      (new File(jarPath)).delete();
    }
  }

  @Override
  public void startSqoop() throws Exception {
    // Do nothing so that Sqoop isn't started before Suite.
  }
}
