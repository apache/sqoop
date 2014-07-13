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

package org.apache.sqoop.orm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Shell;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.util.FileListing;
import com.cloudera.sqoop.util.Jars;

/**
 * Manages the compilation of a bunch of .java files into .class files
 * and eventually a jar.
 *
 * Also embeds this program's jar into the lib/ directory inside the compiled
 * jar to ensure that the job runs correctly.
 */
public class CompilationManager {

  /** If we cannot infer a jar name from a table name, etc., use this. */
  public static final String DEFAULT_CODEGEN_JAR_NAME =
      "sqoop-codegen-created.jar";

  public static final Log LOG = LogFactory.getLog(
      CompilationManager.class.getName());

  private SqoopOptions options;
  private List<String> sources;

  public CompilationManager(final SqoopOptions opts) {
    options = opts;
    sources = new ArrayList<String>();
  }

  public void addSourceFile(String sourceName) {
    sources.add(sourceName);
  }

  /**
   * locate the hadoop-*-core.jar in $HADOOP_MAPRED_HOME or
   * --hadoop-mapred-home.
   * If that doesn't work, check our classpath.
   * @return the filename of the hadoop-*-core.jar file.
   */
  private String findHadoopJars() {
    String hadoopMapRedHome = options.getHadoopMapRedHome();

    if (null == hadoopMapRedHome) {
      LOG.info("$HADOOP_MAPRED_HOME is not set");
      return Jars.getJarPathForClass(JobConf.class);
    }

    if (!hadoopMapRedHome.endsWith(File.separator)) {
      hadoopMapRedHome = hadoopMapRedHome + File.separator;
    }

    File hadoopMapRedHomeFile = new File(hadoopMapRedHome);
    LOG.info("HADOOP_MAPRED_HOME is " + hadoopMapRedHomeFile.getAbsolutePath());
    
    Iterator<File> filesIterator = FileUtils.iterateFiles(hadoopMapRedHomeFile,
          new String[] { "jar" }, true);
    StringBuilder sb = new StringBuilder();

    while (filesIterator.hasNext()) {
      File file = filesIterator.next();
      String name = file.getName();
      if (name.startsWith("hadoop-common")
        || name.startsWith("hadoop-mapreduce-client-core")
        || name.startsWith("hadoop-core")) {
          sb.append(file.getAbsolutePath());
        sb.append(File.pathSeparator);
        }
    }

    if (sb.length() < 1) {
      LOG.warn("HADOOP_MAPRED_HOME appears empty or missing");
      return Jars.getJarPathForClass(JobConf.class);
    }

    String s = sb.substring(0, sb.length() - 1);
    LOG.debug("Returning jar file path " + s);
    return s;
  }

  /**
   * Compile the .java files into .class files via embedded javac call.
   * On success, move .java files to the code output dir.
   */
  public void compile() throws IOException {
    List<String> args = new ArrayList<String>();

    // ensure that the jar output dir exists.
    String jarOutDir = options.getJarOutputDir();
    File jarOutDirObj = new File(jarOutDir);
    if (!jarOutDirObj.exists()) {
      boolean mkdirSuccess = jarOutDirObj.mkdirs();
      if (!mkdirSuccess) {
        LOG.debug("Warning: Could not make directories for " + jarOutDir);
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Found existing " + jarOutDir);
    }

    // Make sure jarOutDir ends with a '/'.
    if (!jarOutDir.endsWith(File.separator)) {
      jarOutDir = jarOutDir + File.separator;
    }

    // find hadoop-*-core.jar for classpath.
    String coreJar = findHadoopJars();
    if (null == coreJar) {
      // Couldn't find a core jar to insert into the CP for compilation.  If,
      // however, we're running this from a unit test, then the path to the
      // .class files might be set via the hadoop.alt.classpath property
      // instead. Check there first.
      String coreClassesPath = System.getProperty("hadoop.alt.classpath");
      if (null == coreClassesPath) {
        // no -- we're out of options. Fail.
        throw new IOException("Could not find hadoop core jar!");
      } else {
        coreJar = coreClassesPath;
      }
    }

    // find sqoop jar for compilation classpath
    String sqoopJar = Jars.getSqoopJarPath();
    if (null != sqoopJar) {
      sqoopJar = File.pathSeparator + sqoopJar;
    } else {
      LOG.warn("Could not find sqoop jar; child compilation may fail");
      sqoopJar = "";
    }

    String curClasspath = System.getProperty("java.class.path");
    LOG.debug("Current sqoop classpath = " + curClasspath);

    args.add("-sourcepath");
    args.add(jarOutDir);

    args.add("-d");
    args.add(jarOutDir);

    args.add("-classpath");
    args.add(curClasspath + File.pathSeparator + coreJar + sqoopJar);

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (null == compiler) {
      LOG.error("It seems as though you are running sqoop with a JRE.");
      LOG.error("Sqoop requires a JDK that can compile Java code.");
      LOG.error("Please install a JDK and set $JAVA_HOME to use it.");
      throw new IOException("Could not start Java compiler.");
    }
    StandardJavaFileManager fileManager =
        compiler.getStandardFileManager(null, null, null);

    ArrayList<String> srcFileNames = new ArrayList<String>();
    for (String srcfile : sources) {
      srcFileNames.add(jarOutDir + srcfile);
      LOG.debug("Adding source file: " + jarOutDir + srcfile);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Invoking javac with args:");
      for (String arg : args) {
        LOG.debug("  " + arg);
      }
    }

    Iterable<? extends JavaFileObject> srcFileObjs =
        fileManager.getJavaFileObjectsFromStrings(srcFileNames);
    JavaCompiler.CompilationTask task = compiler.getTask(
        null, // Write to stderr
        fileManager,
        null, // No special diagnostic handling
        args,
        null, // Compile all classes in the source compilation units
        srcFileObjs);

    boolean result = task.call();
    if (!result) {
      throw new IOException("Error returned by javac");
    }

    // Where we should move source files after compilation.
    String srcOutDir = new File(options.getCodeOutputDir()).getAbsolutePath();
    if (!srcOutDir.endsWith(File.separator)) {
      srcOutDir = srcOutDir + File.separator;
    }

    // Move these files to the srcOutDir.
    for (String srcFileName : sources) {
      String orig = jarOutDir + srcFileName;
      String dest = srcOutDir + srcFileName;
      File fOrig = new File(orig);
      File fDest = new File(dest);
      File fDestParent = fDest.getParentFile();
      if (null != fDestParent && !fDestParent.exists()) {
        if (!fDestParent.mkdirs()) {
          LOG.error("Could not make directory: " + fDestParent);
        }
      }
      try {
          FileUtils.moveFile(fOrig, fDest);
      } catch (IOException e) {
    	  /*Removed the exception being thrown
    	   *even if the .java file can not be renamed
    	   *or can not be moved a "dest" directory for
    	   *any reason.*/
          LOG.debug("Could not rename " + orig + " to " + dest); 
      }
    }
  }

  /**
   * @return the complete filename of the .jar file to generate. */
  public String getJarFilename() {
    String jarOutDir = options.getJarOutputDir();
    String tableName = options.getTableName();
    String specificClassName = options.getClassName();

    if (specificClassName != null && specificClassName.length() > 0) {
      return jarOutDir + specificClassName + ".jar";
    } else if (null != tableName && tableName.length() > 0) {
      return jarOutDir + tableName + ".jar";
    } else if (this.sources.size() == 1) {
      // if we only have one source file, find it's base name,
      // turn "foo.java" into "foo", and then return jarDir + "foo" + ".jar"
      String srcFileName = this.sources.get(0);
      String basename = new File(srcFileName).getName();
      String [] parts = basename.split("\\.");
      String preExtPart = parts[0];
      return jarOutDir + preExtPart + ".jar";
    } else {
      return jarOutDir + DEFAULT_CODEGEN_JAR_NAME;
    }
  }

  /**
   * Searches through a directory and its children for .class
   * files to add to a jar.
   *
   * @param dir - The root directory to scan with this algorithm.
   * @param jstream - The JarOutputStream to write .class files to.
   */
  private void addClassFilesFromDir(File dir, JarOutputStream jstream)
      throws IOException {
    LOG.debug("Scanning for .class files in directory: " + dir);
    List<File> dirEntries = FileListing.getFileListing(dir);
    String baseDirName = dir.getAbsolutePath();
    if (!baseDirName.endsWith(File.separator)) {
      baseDirName = baseDirName + File.separator;
    }

    // For each input class file, create a zipfile entry for it,
    // read the file into a buffer, and write it to the jar file.
    for (File entry : dirEntries) {
      if (!entry.isDirectory()) {
        // Chomp off the portion of the full path that is shared
        // with the base directory where class files were put;
        // we only record the subdir parts in the zip entry.
        String fullPath = entry.getAbsolutePath();
        String chompedPath = fullPath.substring(baseDirName.length());

        boolean include = chompedPath.endsWith(".class")
            && sources.contains(
            chompedPath.substring(0, chompedPath.length() - ".class".length())
            + ".java");

        if (include) {
          // include this file.
          if (Shell.WINDOWS) {
            // In Windows OS, elements in jar files still need to have a path
            // separator of '/' rather than the default File.separator which is '\'
            chompedPath = chompedPath.replace(File.separator, "/");
          }
          LOG.debug("Got classfile: " + entry.getPath() + " -> " + chompedPath);
          ZipEntry ze = new ZipEntry(chompedPath);
          jstream.putNextEntry(ze);
          copyFileToStream(entry, jstream);
          jstream.closeEntry();
        }
      }
    }
  }

  /**
   * Create an output jar file to use when executing MapReduce jobs.
   */
  public void jar() throws IOException {
    String jarOutDir = options.getJarOutputDir();

    String jarFilename = getJarFilename();

    LOG.info("Writing jar file: " + jarFilename);

    File jarFileObj = new File(jarFilename);
    if (jarFileObj.exists()) {
      LOG.debug("Found existing jar (" + jarFilename + "); removing.");
      if (!jarFileObj.delete()) {
        LOG.warn("Could not remove existing jar file: " + jarFilename);
      }
    }

    FileOutputStream fstream = null;
    JarOutputStream jstream = null;
    try {
      fstream = new FileOutputStream(jarFilename);
      jstream = new JarOutputStream(fstream);

      addClassFilesFromDir(new File(jarOutDir), jstream);
      jstream.finish();
    } finally {
      if (null != jstream) {
        try {
          jstream.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing jar stream: " + ioe.toString());
        }
      }

      if (null != fstream) {
        try {
          fstream.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing file stream: " + ioe.toString());
        }
      }
    }

    LOG.debug("Finished writing jar file " + jarFilename);
  }

  private static final int BUFFER_SZ = 4096;

  /**
   * Utility method to copy a .class file into the jar stream.
   * @param f
   * @param ostream
   * @throws IOException
   */
  private void copyFileToStream(File f, OutputStream ostream)
      throws IOException {
    FileInputStream fis = new FileInputStream(f);
    byte [] buffer = new byte[BUFFER_SZ];
    try {
      while (true) {
        int bytesReceived = fis.read(buffer);
        if (bytesReceived < 1) {
          break;
        }

        ostream.write(buffer, 0, bytesReceived);
      }
    } finally {
      fis.close();
    }
  }
}
