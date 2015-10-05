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
package org.apache.sqoop.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.tools.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 *
 */
public class TestClassUtils {

  private ClassLoader classLoader;
  private Path outputDir;
  private Class testAClass;
  private Class testParentClass;
  private Class testChildClass;

  @BeforeMethod
  public void captureClassLoader() throws Exception {
    classLoader = Thread.currentThread().getContextClassLoader();
    File jarFile = compileAJar();
    URL[] urlArray = { jarFile.toURI().toURL() };
    URLClassLoader newClassLoader = new URLClassLoader(urlArray, classLoader);
    testAClass = newClassLoader.loadClass("A");
    testParentClass = newClassLoader.loadClass("Parent");
    testChildClass = newClassLoader.loadClass("Child");
    Thread.currentThread().setContextClassLoader(newClassLoader);
  }

  private File compileAJar() throws Exception{
    String jarName = "test-jar.jar";

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new IllegalStateException(
        "Cannot find the system Java compiler. "
          + "Check that your class path includes tools.jar");
    }

    outputDir = Files.createTempDirectory(null);

    ClassLoader classLoader = getClass().getClassLoader();
    List<File> sourceFiles = new ArrayList<>();
    File sourceFile = new File(classLoader.getResource("TestJar/A.java").getFile());
    sourceFiles.add(sourceFile);
    sourceFile = new File(classLoader.getResource("TestJar/Child.java").getFile());
    sourceFiles.add(sourceFile);
    sourceFile = new File(classLoader.getResource("TestJar/Parent.java").getFile());
    sourceFiles.add(sourceFile);

    StandardJavaFileManager fileManager = compiler.getStandardFileManager
      (null, null, null);

    fileManager.setLocation(StandardLocation.CLASS_OUTPUT,
      Arrays.asList(new File(outputDir.toString())));

    Iterable<? extends JavaFileObject> compilationUnits1 =
      fileManager.getJavaFileObjectsFromFiles(sourceFiles);

    boolean compiled = compiler.getTask(null, fileManager, null, null, null, compilationUnits1).call();
    if (!compiled) {
      throw new RuntimeException("failed to compile");
    }

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, ".");

    JarOutputStream target = new JarOutputStream(new FileOutputStream(outputDir.toString() + File.separator + jarName), manifest);

    List<String> classesForJar = new ArrayList<>();
    //split the file on dot to get the filename from FILENAME.java
    for (File source : sourceFiles) {
      String fileName = source.getName().split("\\.")[0];
      classesForJar.add(fileName);
    }

    File[] directoryListing = outputDir.toFile().listFiles();
    for (File compiledClass : directoryListing) {
      String classFileName = compiledClass.getName().split("\\$")[0].split("\\.")[0];
      if (classesForJar.contains(classFileName)){
        addFileToJar(compiledClass, target);
      }
    }

    target.close();

    //delete non jar files
    for (File file : outputDir.toFile().listFiles()) {
      String extension = file.getName().split("\\.")[1];
      if (!extension.equals("jar")) {
        file.delete();
      }
    }
    return new File(outputDir.toString() + File.separator + jarName);
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

  @AfterMethod
  public void restoreClassLoader() {
    Thread.currentThread().setContextClassLoader(classLoader);
  }

  @Test
  public void testLoadClass() {
    assertNull(ClassUtils.loadClass("IDONTEXIST"));
    assertEquals(testAClass, ClassUtils.loadClass(testAClass.getName()));
  }

  @Test
  public void testLoadClassWithClassLoader() throws Exception {
    String classpath = ClassUtils.jarForClass(testAClass);
    assertNotEquals(testAClass, ClassUtils.loadClassWithClassLoader(testAClass.getName(),
        new ConnectorClassLoader(classpath, getClass().getClassLoader(), Arrays.asList("java."))));
  }

  @Test
  public void testInstantiateNull() {
    assertNull(ClassUtils.instantiate((Class) null));
  }

  @Test
  public void testInstantiate() throws Exception {
    // Just object calls
    Object a = ClassUtils.instantiate(testAClass, "a");
    Field numField = testAClass.getField("num");
    Field aField = testAClass.getField("a");
    Field bField = testAClass.getField("b");
    Field cField = testAClass.getField("c");
    Field pField = testAClass.getField("p");
    assertNotNull(a);
    assertEquals(1, numField.get(a));
    assertEquals("a", aField.get(a));

    // Automatic wrapping primitive -> objects
    Object b = ClassUtils.instantiate(testAClass, "b", 3, 5);
    assertNotNull(b);
    assertEquals(3, numField.get(b));
    assertEquals("b", aField.get(b));
    assertEquals(3, bField.get(b));
    assertEquals(5, cField.get(b));

    // Primitive types in the constructor definition
    Primitive p = (Primitive) ClassUtils.instantiate(Primitive.class, 1, 1.0f, true);
    assertNotNull(p);
    assertEquals(1, p.i);
    assertEquals(1.0f, p.f, 0.0f);
    assertEquals(true, p.b);

    // Subclasses can be used in the constructor call
    Object c = ClassUtils.instantiate(testAClass, ClassUtils.instantiate(testChildClass));
    assertNotNull(c);
    assertNotNull(pField.get(c));
    assertEquals(testChildClass, pField.get(c).getClass());
  }

  public static class Primitive {
    int i;
    float f;
    boolean b;

    public Primitive(int i, float f, boolean b) {
      this.i = i;
      this.f = f;
      this.b = b;
    }
  }

  @Test
  public void testGetEnumStrings() {
    assertEquals(new String[]{}, ClassUtils.getEnumStrings(testAClass));

    assertEquals(
            new String[]{"A", "B", "C"},
            ClassUtils.getEnumStrings(EnumA.class)
    );
    assertEquals(
            new String[]{"X", "Y"},
            ClassUtils.getEnumStrings(EnumX.class)
    );
  }

  enum EnumX {
    X, Y
  }

  enum EnumA {
    A, B, C
  }
}
