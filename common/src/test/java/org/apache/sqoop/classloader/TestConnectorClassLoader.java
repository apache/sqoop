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
package org.apache.sqoop.classloader;

import static org.apache.sqoop.classloader.ConnectorClassLoader.constructUrlsFromClasspath;
import static org.apache.sqoop.classloader.ConnectorClassLoader.isSystemClass;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.sqoop.classloader.ConnectorClassLoader;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class TestConnectorClassLoader {
  private static File testDir = new File(System.getProperty("maven.build.directory",
      System.getProperty("java.io.tmpdir")), "connectorclassloader");

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    FileUtils.deleteQuietly(testDir);
    testDir.mkdirs();
  }

  @Test
  public void testConstructUrlsFromClasspath() throws Exception {
    File file = new File(testDir, "file");
    assertTrue(file.createNewFile(), "Create file");

    File dir = new File(testDir, "dir");
    assertTrue(dir.mkdir(), "Make dir");

    File jarsDir = new File(testDir, "jarsdir");
    assertTrue(jarsDir.mkdir(), "Make jarsDir");
    File nonJarFile = new File(jarsDir, "nonjar");
    assertTrue(nonJarFile.createNewFile(), "Create non-jar file");
    File jarFile = new File(jarsDir, "a.jar");
    assertTrue(jarFile.createNewFile(), "Create jar file");

    File nofile = new File(testDir, "nofile");
    // don't create nofile

    StringBuilder cp = new StringBuilder();
    cp.append(file.getAbsolutePath()).append(File.pathSeparator)
      .append(dir.getAbsolutePath()).append(File.pathSeparator)
      .append(jarsDir.getAbsolutePath() + "/*").append(File.pathSeparator)
      .append(nofile.getAbsolutePath()).append(File.pathSeparator)
      .append(nofile.getAbsolutePath() + "/*").append(File.pathSeparator);

    URL[] urls = constructUrlsFromClasspath(cp.toString());

    assertEquals(3, urls.length);
    assertEquals(file.toURI().toURL(), urls[0]);
    assertEquals(dir.toURI().toURL(), urls[1]);
    assertEquals(jarFile.toURI().toURL(), urls[2]);
    // nofile should be ignored
  }

  @Test
  public void testIsSystemClass() {
    testIsSystemClassInternal("");
  }

  @Test
  public void testIsSystemNestedClass() {
    testIsSystemClassInternal("$Klass");
  }

  private void testIsSystemClassInternal(String nestedClass) {
    assertFalse(isSystemClass("org.example.Foo" + nestedClass, null));
    assertTrue(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.Foo")));
    assertTrue(isSystemClass("/org.example.Foo" + nestedClass,
        classes("org.example.Foo")));
    assertTrue(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.")));
    assertTrue(isSystemClass("net.example.Foo" + nestedClass,
        classes("org.example.,net.example.")));
    assertFalse(isSystemClass("org.example.Foo" + nestedClass,
        classes("-org.example.Foo,org.example.")));
    assertTrue(isSystemClass("org.example.Bar" + nestedClass,
        classes("-org.example.Foo.,org.example.")));
    assertFalse(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.,-org.example.Foo")));
    assertFalse(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.Foo,-org.example.Foo")));
  }

  private List<String> classes(String classes) {
    return Lists.newArrayList(Splitter.on(',').split(classes));
  }

  @Test
  public void testGetResource() throws IOException {
    URL testJar = makeTestJar().toURI().toURL();

    ClassLoader currentClassLoader = getClass().getClassLoader();
    ClassLoader connectorClassloader = new ConnectorClassLoader(
        new URL[] { testJar }, currentClassLoader, null, false);

    assertNull(currentClassLoader.getResourceAsStream("resource.txt"),
        "Resource should be null for current classloader");
    assertNull(currentClassLoader.getResourceAsStream("resource-dep.txt"),
        "Resource should be null for current classloader");

    InputStream in = connectorClassloader.getResourceAsStream("resource.txt");
    assertNotNull(in, "Resource should not be null for connector classloader");

    in = connectorClassloader.getResourceAsStream("resource-dep.txt");
    assertNotNull(in, "Resource should not be null for connector classloader");
    assertEquals(IOUtils.toString(in), "hello dep");
  }

  @Test
  public void testGetResources() throws IOException {
    URL testJar = makeTestJar().toURI().toURL();

    ClassLoader currentClassLoader = getClass().getClassLoader();
    ClassLoader connectorClassloader = new ConnectorClassLoader(
        new URL[] { testJar }, currentClassLoader, null, false);

    List<String> resourceContents = new ArrayList<String>();
    resourceContents.add("hello A");
    resourceContents.add("hello B");
    Enumeration<URL> urlEnum = connectorClassloader.getResources("resource.txt");
    assertTrue(urlEnum.hasMoreElements());
    resourceContents.remove(IOUtils.toString(urlEnum.nextElement().openStream()));

    assertTrue(urlEnum.hasMoreElements());
    resourceContents.remove(IOUtils.toString(urlEnum.nextElement().openStream()));

    assertEquals(resourceContents.size(), 0);
  }

  @Test
  public void testLoadClass() throws Exception {
    URL testJar = makeTestJar().toURI().toURL();

    ClassLoader currentClassLoader = getClass().getClassLoader();
    ClassLoader connectorClassloader = new ConnectorClassLoader(
        new URL[] { testJar }, currentClassLoader, null, false);

    try {
      currentClassLoader.loadClass("A");
      fail("Should throw ClassNotFoundException when loading class A");
    } catch (ClassNotFoundException e) {
      // Expected
    }
    try {
      currentClassLoader.loadClass("B");
      fail("Should throw ClassNotFoundException when loading class B");
    } catch (ClassNotFoundException e) {
      // Expected
    }

    assertNotNull(connectorClassloader.loadClass("A"));
    assertNotNull(connectorClassloader.loadClass("B"));
  }

  private File makeTestJar() throws IOException{
    File jarFile = new File(testDir, "test.jar");

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, ".");

    // Create test.jar
    JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile), manifest);
    List<String> classFiles = new ArrayList<String>();
    classFiles.add("TestConnectorClassLoader/A.java");
    addFilesToJar(classFiles, jos);
    JarEntry entry = new JarEntry("resource.txt");
    addEntry(new ByteArrayInputStream("hello A".getBytes()), jos, entry);

    // Create lib/test-dep.jar
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JarOutputStream depJos = new JarOutputStream(baos, manifest);
    classFiles.clear();
    classFiles.add("TestConnectorClassLoader/lib/B.java");
    addFilesToJar(classFiles, depJos);
    entry = new JarEntry("resource.txt");
    addEntry(new ByteArrayInputStream("hello B".getBytes()), depJos, entry);
    entry = new JarEntry("resource-dep.txt");
    addEntry(new ByteArrayInputStream("hello dep".getBytes()), depJos, entry);
    depJos.close();

    // Add lib/test-dep.jar to test.jar
    entry = new JarEntry("lib/test-dep.jar");
    addEntry(new ByteArrayInputStream(baos.toByteArray()), jos, entry);

    jos.close();
    return jarFile;
  }

  private void addFilesToJar(List<String> classFiles, JarOutputStream jos) throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    List<File> sourceFiles = new ArrayList<>();
    for (String classFile : classFiles) {
      File sourceFile = new File(classLoader.getResource(classFile).getFile());
      sourceFiles.add(sourceFile);
    }

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new IllegalStateException(
        "Cannot find the system Java compiler. "
          + "Check that your class path includes tools.jar");
    }
    StandardJavaFileManager fileManager = compiler.getStandardFileManager
      (null, null, null);

    fileManager.setLocation(StandardLocation.CLASS_OUTPUT, Arrays.asList(testDir));

    Iterable<? extends JavaFileObject> compilationUnits1 =
      fileManager.getJavaFileObjectsFromFiles(sourceFiles);

    boolean compiled = compiler.getTask(null, fileManager, null, null, null, compilationUnits1).call();
    if (!compiled) {
      throw new RuntimeException("failed to compile");
    }

    List<String> classesForJar = new ArrayList<String>();
    // Split the file on dot to get the filename from FILENAME.java
    for (File source : sourceFiles) {
      String fileName = source.getName().split("\\.")[0];
      classesForJar.add(fileName);
    }

    File[] directoryListing = testDir.listFiles();
    for (File compiledClass : directoryListing) {
      String classFileName = compiledClass.getName().split("\\$")[0].split("\\.")[0];
      if (classesForJar.contains(classFileName)){
        addFileToJar(compiledClass, jos);
      }
    }
  }

  private void addFileToJar(File source, JarOutputStream jos) throws IOException {
    JarEntry entry = new JarEntry(source.getName());
    entry.setTime(source.lastModified());

    BufferedInputStream in = new BufferedInputStream(new FileInputStream(source));
    addEntry(in, jos, entry);

    if (in != null) {
      in.close();
    }
  }

  private void addEntry(InputStream in, JarOutputStream jos, JarEntry entry) throws IOException {
    jos.putNextEntry(entry);

    byte[] buffer = new byte[1024];
    while (true) {
      int count = in.read(buffer);
      if (count == -1) {
        break;
      }
      jos.write(buffer, 0, count);
    }

    jos.closeEntry();
  }
}
