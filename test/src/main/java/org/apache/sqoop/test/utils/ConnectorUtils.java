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
package org.apache.sqoop.test.utils;

import org.apache.commons.collections.ListUtils;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class ConnectorUtils {

  static class JarContents {
    private List<File> sourceFiles;
    private List<File> properitesFiles;
    private List<String> dependencyJarFiles;

    public JarContents(List<File> sourceFiles, List<File> properitesFiles, List<String> dependencyJarFiles) {
      this.sourceFiles = sourceFiles;
      this.properitesFiles = properitesFiles;
      this.dependencyJarFiles = dependencyJarFiles;
    }

    public List<File> getSourceFiles() {
      return sourceFiles;
    }

    public List<File> getProperitesFiles() {
      return properitesFiles;
    }

    public List<String> getDependencyJarFiles() {
      return dependencyJarFiles;
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, String> compileTestConnectorAndDependency(String[] connectorSourceFiles,
      String[] connectorDependencySourceFiles, String[] connectorPropertyFiles, String connectorJarName,
      String connectorDependencyJarName, boolean dependencyBuiltInsideConnectorJar) throws Exception {
    ClassLoader classLoader = ConnectorUtils.class.getClassLoader();
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new IllegalStateException(
              "Cannot find the system Java compiler. "
                      + "Check that your class path includes tools.jar");
    }

    Path outputDir = Files.createTempDirectory(null);

    Map<String, JarContents> sourceFileToJarMap = new LinkedHashMap<>();

    List<File> sourceFiles = new ArrayList<>();

    for (String connectorDependencySourceFile : connectorDependencySourceFiles) {
      File file = new File(classLoader.getResource(connectorDependencySourceFile).getFile());
      sourceFiles.add(file);
    }
    if (connectorDependencySourceFiles.length > 0) {
      sourceFileToJarMap.put(connectorDependencyJarName, new JarContents(sourceFiles, ListUtils.EMPTY_LIST, ListUtils.EMPTY_LIST));
    }

    sourceFiles = new ArrayList<>();
    for (String connectorSourceFile : connectorSourceFiles) {
      File file = new File(classLoader.getResource(connectorSourceFile).getFile());
      sourceFiles.add(file);
    }

    List<File> propertiesFiles = new ArrayList<>();
    for (String connectorPropertyFile : connectorPropertyFiles) {
      File file = new File(classLoader.getResource(connectorPropertyFile).getFile());
      propertiesFiles.add(file);
    }

    List<String> dependencyFiles = new ArrayList<>();
    if (dependencyBuiltInsideConnectorJar) {
      dependencyFiles.add(connectorDependencyJarName);
    }
    sourceFileToJarMap.put(connectorJarName, new JarContents(sourceFiles, propertiesFiles, dependencyFiles));

    buildJar(outputDir.toString(), sourceFileToJarMap);

    Map<String, String> jarMap = new HashMap<>();
    jarMap.put(connectorJarName, outputDir.toString() + File.separator + connectorJarName);
    jarMap.put(connectorDependencyJarName, outputDir.toString() + File.separator + connectorDependencyJarName);
    return jarMap;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  private static void buildJar(String outputDir, Map<String, JarContents> sourceFileToJarMap) throws IOException {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager = compiler.getStandardFileManager
            (null, null, null);

    List<File> sourceFiles = new ArrayList<>();
    for(JarContents jarContents : sourceFileToJarMap.values()) {
      sourceFiles.addAll(jarContents.sourceFiles);
    }

    fileManager.setLocation(StandardLocation.CLASS_OUTPUT,
            Arrays.asList(new File(outputDir)));

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

      JarOutputStream target = new JarOutputStream(new FileOutputStream(outputDir + File.separator + jarNameAndContents.getKey()), manifest);
      List<String> classesForJar = new ArrayList<>();
      for(File sourceFile : jarNameAndContents.getValue().getSourceFiles()) {
        //split the file on dot to get the filename from FILENAME.java
        String fileName = sourceFile.getName().split("\\.")[0];
        classesForJar.add(fileName);
      }

      File dir = new File(outputDir);
      File[] directoryListing = dir.listFiles();
      if (directoryListing != null) {
        for (File compiledClass : directoryListing) {
          String classFileName = compiledClass.getName().split("\\$")[0].split("\\.")[0];
          if (classesForJar.contains(classFileName)){
            addFileToJar(compiledClass, target);
          }
        }
      }

      for (File propertiesFile : jarNameAndContents.getValue().getProperitesFiles()) {
        addFileToJar(propertiesFile, target);
      }

      for (String dependencyJarFileName : jarNameAndContents.getValue().getDependencyJarFiles()) {
        File dependencyJarFile = new File(dir, dependencyJarFileName);
        addFileToJar(dependencyJarFile, target);
      }

      target.close();
    }
    //delete non jar files
    File dir = new File(outputDir);
    File[] directoryListing = dir.listFiles();
    if (directoryListing != null) {
      for (File file : directoryListing) {
        String extension = file.getName().split("\\.")[1];
        if (!extension.equals("jar")) {
          file.delete();
        }
      }
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings("OS_OPEN_STREAM_EXCEPTION_PATH")
  private static void addFileToJar(File source, JarOutputStream target) throws IOException {
    String entryName;
    if (source.getName().endsWith(".jar")) {
      // put dependency jars into directory "lib"
      entryName = "lib/" + source.getName();
    } else {
      entryName = source.getName();
    }
    JarEntry entry = new JarEntry(entryName);
    entry.setTime(source.lastModified());
    target.putNextEntry(entry);
    BufferedInputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(source));

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
    } finally {
      target.closeEntry();
      if (in != null) {
        in.close();
      }
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  public static void deleteJars(Map<String, String> jarMap) {
    for (String jarPath : jarMap.values()) {
      (new File(jarPath)).delete();
    }
  }

}
