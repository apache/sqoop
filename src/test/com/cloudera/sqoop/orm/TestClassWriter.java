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

package com.cloudera.sqoop.orm;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.TestConnFactory.DummyManager;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.testutil.DirUtil;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.util.ClassLoaderStack;

import java.lang.reflect.Field;

/**
 * Test that the ClassWriter generates Java classes based on the given table,
 * which compile.
 */
public class TestClassWriter extends TestCase {

  public static final Log LOG =
      LogFactory.getLog(TestClassWriter.class.getName());

  // instance variables populated during setUp, used during tests
  private HsqldbTestServer testServer;
  private ConnManager manager;
  private SqoopOptions options;

  @Before
  public void setUp() {
    testServer = new HsqldbTestServer();
    org.apache.log4j.Logger root = org.apache.log4j.Logger.getRootLogger();
    root.setLevel(org.apache.log4j.Level.DEBUG);
    try {
      testServer.resetServer();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    } catch (ClassNotFoundException cnfe) {
      LOG.error("Could not find class for db driver: " + cnfe.toString());
      fail("Could not find class for db driver: " + cnfe.toString());
    }

    manager = testServer.getManager();
    options = testServer.getSqoopOptions();

    // sanity check: make sure we're in a tmp dir before we blow anything away.
    assertTrue("Test generates code in non-tmp dir!",
        CODE_GEN_DIR.startsWith(ImportJobTestCase.TEMP_BASE_DIR));
    assertTrue("Test generates jars in non-tmp dir!",
        JAR_GEN_DIR.startsWith(ImportJobTestCase.TEMP_BASE_DIR));

    // start out by removing these directories ahead of time
    // to ensure that this is truly generating the code.
    File codeGenDirFile = new File(CODE_GEN_DIR);
    File classGenDirFile = new File(JAR_GEN_DIR);

    if (codeGenDirFile.exists()) {
      LOG.debug("Removing code gen dir: " + codeGenDirFile);
      if (!DirUtil.deleteDir(codeGenDirFile)) {
        LOG.warn("Could not delete " + codeGenDirFile + " prior to test");
      }
    }

    if (classGenDirFile.exists()) {
      LOG.debug("Removing class gen dir: " + classGenDirFile);
      if (!DirUtil.deleteDir(classGenDirFile)) {
        LOG.warn("Could not delete " + classGenDirFile + " prior to test");
      }
    }
  }

  @After
  public void tearDown() {
    try {
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  static final String CODE_GEN_DIR = ImportJobTestCase.TEMP_BASE_DIR
      + "sqoop/test/codegen";
  static final String JAR_GEN_DIR = ImportJobTestCase.TEMP_BASE_DIR
      + "sqoop/test/jargen";

  /**
   * Run a test to verify that we can generate code and it emits the output
   * files where we expect them.
   * @return
   */
  private File runGenerationTest(String [] argv, String classNameToCheck) {
    File codeGenDirFile = new File(CODE_GEN_DIR);
    File classGenDirFile = new File(JAR_GEN_DIR);

    try {
      options = new ImportTool().parseArguments(argv,
          null, options, true);
    } catch (Exception e) {
      LOG.error("Could not parse options: " + e.toString());
    }

    CompilationManager compileMgr = new CompilationManager(options);
    ClassWriter writer = new ClassWriter(options, manager,
        HsqldbTestServer.getTableName(), compileMgr);

    try {
      writer.generate();
      compileMgr.compile();
      compileMgr.jar();
    } catch (IOException ioe) {
      LOG.error("Got IOException: " + ioe.toString());
      fail("Got IOException: " + ioe.toString());
    }

    String classFileNameToCheck = classNameToCheck.replace('.',
        File.separatorChar);
    LOG.debug("Class file to check for: " + classFileNameToCheck);

    // Check that all the files we expected to generate (.java, .class, .jar)
    // exist.
    File tableFile = new File(codeGenDirFile, classFileNameToCheck + ".java");
    assertTrue("Cannot find generated source file for table!",
        tableFile.exists());
    LOG.debug("Found generated source: " + tableFile);

    File tableClassFile = new File(classGenDirFile, classFileNameToCheck
        + ".class");
    assertTrue("Cannot find generated class file for table!",
        tableClassFile.exists());
    LOG.debug("Found generated class: " + tableClassFile);

    File jarFile = new File(compileMgr.getJarFilename());
    assertTrue("Cannot find compiled jar", jarFile.exists());
    LOG.debug("Found generated jar: " + jarFile);

    // check that the .class file made it into the .jar by enumerating
    // available entries in the jar file.
    boolean foundCompiledClass = false;
    if (Shell.WINDOWS) {
        // In Windows OS, elements in jar files still need to have a path
        // separator of '/' rather than the default File.separator which is '\'
        classFileNameToCheck = classFileNameToCheck.replace(File.separator, "/");
    }
    try {
      JarInputStream jis = new JarInputStream(new FileInputStream(jarFile));

      LOG.debug("Jar file has entries:");
      while (true) {
        JarEntry entry = jis.getNextJarEntry();
        if (null == entry) {
          // no more entries.
          break;
        }

        if (entry.getName().equals(classFileNameToCheck + ".class")) {
          foundCompiledClass = true;
          LOG.debug(" * " + entry.getName());
        } else {
          LOG.debug("   " + entry.getName());
        }
      }

      jis.close();
    } catch (IOException ioe) {
      fail("Got IOException iterating over Jar file: " + ioe.toString());
    }

    assertTrue("Cannot find .class file " + classFileNameToCheck
        + ".class in jar file", foundCompiledClass);

    LOG.debug("Found class in jar - test success!");
    return jarFile;
  }

  /**
   * Test that we can generate code. Test that we can redirect the --outdir
   * and --bindir too.
   */
  @Test
  public void testCodeGen() {

    // Set the option strings in an "argv" to redirect our srcdir and bindir.
    String [] argv = {
      "--bindir",
      JAR_GEN_DIR,
      "--outdir",
      CODE_GEN_DIR,
    };

    runGenerationTest(argv, HsqldbTestServer.getTableName());
  }

  private static final String OVERRIDE_CLASS_NAME = "override";

  /**
   * Test that we can generate code with a custom class name.
   */
  @Test
  public void testSetClassName() {

    // Set the option strings in an "argv" to redirect our srcdir and bindir
    String [] argv = {
      "--bindir",
      JAR_GEN_DIR,
      "--outdir",
      CODE_GEN_DIR,
      "--class-name",
      OVERRIDE_CLASS_NAME,
    };

    runGenerationTest(argv, OVERRIDE_CLASS_NAME);
  }

  private static final String OVERRIDE_CLASS_AND_PACKAGE_NAME =
      "override.pkg.prefix.classname";

  /**
   * Test that we can generate code with a custom class name that includes a
   * package.
   */
  @Test
  public void testSetClassAndPackageName() {

    // Set the option strings in an "argv" to redirect our srcdir and bindir
    String [] argv = {
      "--bindir",
      JAR_GEN_DIR,
      "--outdir",
      CODE_GEN_DIR,
      "--class-name",
      OVERRIDE_CLASS_AND_PACKAGE_NAME,
    };

    runGenerationTest(argv, OVERRIDE_CLASS_AND_PACKAGE_NAME);
  }

  private static final String OVERRIDE_PACKAGE_NAME =
      "special.userpackage.name";

  /**
   * Test that we can generate code with a custom class name that includes a
   * package.
   */
  @Test
  public void testSetPackageName() {

    // Set the option strings in an "argv" to redirect our srcdir and bindir
    String [] argv = {
      "--bindir",
      JAR_GEN_DIR,
      "--outdir",
      CODE_GEN_DIR,
      "--package-name",
      OVERRIDE_PACKAGE_NAME,
    };

    runGenerationTest(argv, OVERRIDE_PACKAGE_NAME + "."
        + HsqldbTestServer.getTableName());
  }


  // Test the SQL identifier -> Java identifier conversion.
  @Test
  public void testJavaIdentifierConversion() {
    assertNull(ClassWriter.getIdentifierStrForChar(' '));
    assertNull(ClassWriter.getIdentifierStrForChar('\t'));
    assertNull(ClassWriter.getIdentifierStrForChar('\r'));
    assertNull(ClassWriter.getIdentifierStrForChar('\n'));
    assertEquals("x", ClassWriter.getIdentifierStrForChar('x'));
    assertEquals("_", ClassWriter.getIdentifierStrForChar('-'));
    assertEquals("_", ClassWriter.getIdentifierStrForChar('_'));

    assertEquals("foo", ClassWriter.toJavaIdentifier("foo"));

    assertEquals("_abstract", ClassWriter.toJavaIdentifier("abstract"));
    assertEquals("_assert", ClassWriter.toJavaIdentifier("assert"));
    assertEquals("_boolean", ClassWriter.toJavaIdentifier("boolean"));
    assertEquals("_break", ClassWriter.toJavaIdentifier("break"));
    assertEquals("_byte", ClassWriter.toJavaIdentifier("byte"));
    assertEquals("_case", ClassWriter.toJavaIdentifier("case"));
    assertEquals("_catch", ClassWriter.toJavaIdentifier("catch"));
    assertEquals("_char", ClassWriter.toJavaIdentifier("char"));
    assertEquals("_class", ClassWriter.toJavaIdentifier("class"));
    assertEquals("_const", ClassWriter.toJavaIdentifier("const"));
    assertEquals("_continue", ClassWriter.toJavaIdentifier("continue"));
    assertEquals("_default", ClassWriter.toJavaIdentifier("default"));
    assertEquals("_do", ClassWriter.toJavaIdentifier("do"));
    assertEquals("_double", ClassWriter.toJavaIdentifier("double"));
    assertEquals("_else", ClassWriter.toJavaIdentifier("else"));
    assertEquals("_enum", ClassWriter.toJavaIdentifier("enum"));
    assertEquals("_extends", ClassWriter.toJavaIdentifier("extends"));
    assertEquals("_false", ClassWriter.toJavaIdentifier("false"));
    assertEquals("_final", ClassWriter.toJavaIdentifier("final"));
    assertEquals("_finally", ClassWriter.toJavaIdentifier("finally"));
    assertEquals("_float", ClassWriter.toJavaIdentifier("float"));
    assertEquals("_for", ClassWriter.toJavaIdentifier("for"));
    assertEquals("_goto", ClassWriter.toJavaIdentifier("goto"));
    assertEquals("_if", ClassWriter.toJavaIdentifier("if"));
    assertEquals("_implements", ClassWriter.toJavaIdentifier("implements"));
    assertEquals("_import", ClassWriter.toJavaIdentifier("import"));
    assertEquals("_instanceof", ClassWriter.toJavaIdentifier("instanceof"));
    assertEquals("_int", ClassWriter.toJavaIdentifier("int"));
    assertEquals("_interface", ClassWriter.toJavaIdentifier("interface"));
    assertEquals("_long", ClassWriter.toJavaIdentifier("long"));
    assertEquals("_native", ClassWriter.toJavaIdentifier("native"));
    assertEquals("_new", ClassWriter.toJavaIdentifier("new"));
    assertEquals("_null", ClassWriter.toJavaIdentifier("null"));
    assertEquals("_package", ClassWriter.toJavaIdentifier("package"));
    assertEquals("_private", ClassWriter.toJavaIdentifier("private"));
    assertEquals("_protected", ClassWriter.toJavaIdentifier("protected"));
    assertEquals("_public", ClassWriter.toJavaIdentifier("public"));
    assertEquals("_return", ClassWriter.toJavaIdentifier("return"));
    assertEquals("_short", ClassWriter.toJavaIdentifier("short"));
    assertEquals("_static", ClassWriter.toJavaIdentifier("static"));
    assertEquals("_strictfp", ClassWriter.toJavaIdentifier("strictfp"));
    assertEquals("_super", ClassWriter.toJavaIdentifier("super"));
    assertEquals("_switch", ClassWriter.toJavaIdentifier("switch"));
    assertEquals("_synchronized", ClassWriter.toJavaIdentifier("synchronized"));
    assertEquals("_this", ClassWriter.toJavaIdentifier("this"));
    assertEquals("_throw", ClassWriter.toJavaIdentifier("throw"));
    assertEquals("_throws", ClassWriter.toJavaIdentifier("throws"));
    assertEquals("_transient", ClassWriter.toJavaIdentifier("transient"));
    assertEquals("_true", ClassWriter.toJavaIdentifier("true"));
    assertEquals("_try", ClassWriter.toJavaIdentifier("try"));
    assertEquals("_void", ClassWriter.toJavaIdentifier("void"));
    assertEquals("_volatile", ClassWriter.toJavaIdentifier("volatile"));
    assertEquals("_while", ClassWriter.toJavaIdentifier("while"));

    assertEquals("_class", ClassWriter.toJavaIdentifier("cla ss"));
    assertEquals("_int", ClassWriter.toJavaIdentifier("int"));
    assertEquals("thisismanywords", ClassWriter.toJavaIdentifier(
        "this is many words"));
    assertEquals("_9isLegalInSql", ClassWriter.toJavaIdentifier(
        "9isLegalInSql"));
    assertEquals("____", ClassWriter.toJavaIdentifier("___"));
    assertEquals("__class", ClassWriter.toJavaIdentifier("_class"));
  }

  @Test
  public void testWeirdColumnNames() throws SQLException {
    // Recreate the table with column names that aren't legal Java identifiers.
    String tableName = HsqldbTestServer.getTableName();
    Connection connection = testServer.getConnection();
    Statement st = connection.createStatement();
    try {
      st.executeUpdate("DROP TABLE " + tableName + " IF EXISTS");
      st.executeUpdate("CREATE TABLE " + tableName
          + " (class INT, \"9field\" INT)");
      st.executeUpdate("INSERT INTO " + tableName + " VALUES(42, 41)");
      connection.commit();
    } finally {
      st.close();
      connection.close();
    }

    String [] argv = {
      "--bindir",
      JAR_GEN_DIR,
      "--outdir",
      CODE_GEN_DIR,
      "--package-name",
      OVERRIDE_PACKAGE_NAME,
    };

    runGenerationTest(argv, OVERRIDE_PACKAGE_NAME + "."
        + HsqldbTestServer.getTableName());
  }

  @Test
  public void testCloningTableWithVarbinaryDoesNotThrowNPE() throws SQLException,
      IOException, ClassNotFoundException, NoSuchMethodException,
      SecurityException, InstantiationException, IllegalAccessException,
      IllegalArgumentException, InvocationTargetException {
    String tableName = HsqldbTestServer.getTableName();
    Connection connection = testServer.getConnection();
    Statement st = connection.createStatement();
    try {
      st.executeUpdate("DROP TABLE " + tableName + " IF EXISTS");
      st.executeUpdate("CREATE TABLE " + tableName
          + " (id INT, test VARBINARY(10))");
      connection.commit();
    } finally {
      st.close();
      connection.close();
    }

    String [] argv = {
      "--bindir",
      JAR_GEN_DIR,
      "--outdir",
      CODE_GEN_DIR,
      "--package-name",
      OVERRIDE_PACKAGE_NAME,
    };

    String className = OVERRIDE_PACKAGE_NAME + "."
        + HsqldbTestServer.getTableName();
    File ormJarFile = runGenerationTest(argv, className);

    ClassLoader prevClassLoader = ClassLoaderStack.addJarFile(
        ormJarFile.getCanonicalPath(), className);
    Class tableClass = Class.forName(className, true,
        Thread.currentThread().getContextClassLoader());
    Method cloneImplementation = tableClass.getMethod("clone");

    Object instance = tableClass.newInstance();

    assertTrue(cloneImplementation.invoke(instance).getClass().
        getCanonicalName().equals(className));

    if (null != prevClassLoader) {
      ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
    }
  }

  /**
   * Test the generated equals method.
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws NoSuchMethodException
   * @throws SecurityException
   * @throws InvocationTargetException
   * @throws IllegalArgumentException
   */
  @Test
  public void testEqualsMethod() throws IOException, ClassNotFoundException,
      InstantiationException, IllegalAccessException, NoSuchMethodException,
      InvocationTargetException {

    // Set the option strings in an "argv" to redirect our srcdir and bindir
    String [] argv = {
      "--bindir",
      JAR_GEN_DIR,
      "--outdir",
      CODE_GEN_DIR,
      "--class-name",
      OVERRIDE_CLASS_AND_PACKAGE_NAME,
    };

    File ormJarFile = runGenerationTest(argv, OVERRIDE_CLASS_AND_PACKAGE_NAME);
    ClassLoader prevClassLoader = ClassLoaderStack.addJarFile(
        ormJarFile.getCanonicalPath(),
        OVERRIDE_CLASS_AND_PACKAGE_NAME);
    Class tableClass = Class.forName(
        OVERRIDE_CLASS_AND_PACKAGE_NAME,
        true,
        Thread.currentThread().getContextClassLoader());
    Method setterIntField1 =
        tableClass.getMethod("set_INTFIELD1", Integer.class);
    Method setterIntField2 =
        tableClass.getMethod("set_INTFIELD2", Integer.class);
    Method equalsImplementation = tableClass.getMethod("equals", Object.class);

    Object instance1 = tableClass.newInstance();
    Object instance2 = tableClass.newInstance();

    // test reflexivity
    assertTrue((Boolean) equalsImplementation.invoke(instance1, instance1));

    // test equality for uninitialized fields
    assertTrue((Boolean) equalsImplementation.invoke(instance1, instance2));

    // test symmetry
    assertTrue((Boolean) equalsImplementation.invoke(instance2, instance1));

    // test reflexivity with initialized fields
    setterIntField1.invoke(instance1, new Integer(1));
    setterIntField2.invoke(instance1, new Integer(2));
    assertTrue((Boolean) equalsImplementation.invoke(instance1, instance1));

    // test difference in both fields
    setterIntField1.invoke(instance2, new Integer(3));
    setterIntField2.invoke(instance2, new Integer(4));
    assertFalse((Boolean) equalsImplementation.invoke(instance1, instance2));

    // test difference in second field
    setterIntField1.invoke(instance2, new Integer(1));
    setterIntField2.invoke(instance2, new Integer(3));
    assertFalse((Boolean) equalsImplementation.invoke(instance1, instance2));

    // test difference in first field
    setterIntField1.invoke(instance2, new Integer(3));
    setterIntField2.invoke(instance2, new Integer(2));
    assertFalse((Boolean) equalsImplementation.invoke(instance1, instance2));

    // test equality for initialized fields
    setterIntField1.invoke(instance2, new Integer(1));
    setterIntField2.invoke(instance2, new Integer(2));
    assertTrue((Boolean) equalsImplementation.invoke(instance1, instance2));

    if (null != prevClassLoader) {
      ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
    }
  }

  private static final String USERMAPPING_CLASS_AND_PACKAGE_NAME =
      "usermapping.pkg.prefix.classname";

  @Test
  public void testUserMapping() throws IOException, ClassNotFoundException,
      InstantiationException, IllegalAccessException, NoSuchMethodException,
      InvocationTargetException {

    // Set the option strings in an "argv" to redirect our srcdir and bindir
    String [] argv = {
      "--bindir", JAR_GEN_DIR,
      "--outdir", CODE_GEN_DIR,
      "--class-name", USERMAPPING_CLASS_AND_PACKAGE_NAME,
      "--map-column-java", "INTFIELD1=String",
    };

    File ormJarFile = runGenerationTest(argv,
            USERMAPPING_CLASS_AND_PACKAGE_NAME);
    ClassLoader prevClassLoader = ClassLoaderStack.addJarFile(
        ormJarFile.getCanonicalPath(),
        USERMAPPING_CLASS_AND_PACKAGE_NAME);
    Class tableClass = Class.forName(
        USERMAPPING_CLASS_AND_PACKAGE_NAME,
        true,
        Thread.currentThread().getContextClassLoader());

    try {
      Field intfield = tableClass.getDeclaredField("INTFIELD1");

      assertEquals(String.class, intfield.getType());
    } catch (NoSuchFieldException ex) {
      fail("Can't find field for INTFIELD1");
    } catch (SecurityException ex) {
      fail("Can't find field for INTFIELD1");
    }

    if (null != prevClassLoader) {
      ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
    }
  }

  @Test
  public void testBrokenUserMapping() throws Exception {

    String [] argv = {
        "--bindir", JAR_GEN_DIR,
        "--outdir", CODE_GEN_DIR,
        "--class-name", USERMAPPING_CLASS_AND_PACKAGE_NAME,
        "--map-column-java", "INTFIELD1=NotARealClass",
    };

    try {
      runGenerationTest(
        argv,
        USERMAPPING_CLASS_AND_PACKAGE_NAME);
    } catch(IllegalArgumentException e) {
      return;
    }
    fail("we shouldn't successfully generate code");
  }

  private void runFailedGenerationTest(String [] argv,
      String classNameToCheck) {
    File codeGenDirFile = new File(CODE_GEN_DIR);
    File classGenDirFile = new File(JAR_GEN_DIR);

    try {
      options = new ImportTool().parseArguments(argv,
          null, options, true);
    } catch (Exception e) {
      LOG.error("Could not parse options: " + e.toString());
    }

    CompilationManager compileMgr = new CompilationManager(options);
    ClassWriter writer = new ClassWriter(options, manager,
        HsqldbTestServer.getTableName(), compileMgr);

    try {
      writer.generate();
      compileMgr.compile();
      fail("ORM class file generation succeeded when it was expected to fail");
    } catch (Exception ioe) {
      LOG.error("Got Exception from ORM generation as expected : "
        + ioe.toString());
    }
  }
  /**
   * A dummy manager that declares that it ORM is self managed.
   */
  public static class DummyDirectManager extends DummyManager {
    @Override
    public boolean isORMFacilitySelfManaged() {
      return true;
    }
  }

  @Test
  public void testNoClassGeneration() throws Exception {
    manager = new DummyDirectManager();
    String [] argv = {
      "--bindir",
      JAR_GEN_DIR,
      "--outdir",
      CODE_GEN_DIR,
    };
    runFailedGenerationTest(argv, HsqldbTestServer.getTableName());
  }
}
