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
package org.apache.sqoop.manager.sqlserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils.*;
import org.apache.sqoop.manager.sqlserver.MSSQLTestDataFileParser.DATATYPES;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.tool.CodeGenTool;
import com.cloudera.sqoop.util.ClassLoaderStack;

/**
* Export sequence file to SQL Server test.
*/
public class SQLServerDatatypeExportSequenceFileManualTest
    extends ManagerCompatExport {

  private static Map jars = new HashMap();

   @Override
  public void createFile(DATATYPES dt, String[] data) throws Exception {
    try {
      codeGen(dt);
      // Instantiate the value record object via reflection.
      Class cls = Class.forName(getTableName(dt), true, Thread
        .currentThread().getContextClassLoader());
      SqoopRecord record = (SqoopRecord) ReflectionUtils.newInstance(cls,
        new Configuration());

      // Create the SequenceFile.
      Configuration conf = new Configuration();
      String hdfsroot;
      hdfsroot = System.getProperty("ms.datatype.test.hdfsprefix");
      if (hdfsroot == null){
        hdfsroot ="hdfs://localhost/";
      }
      conf.set("fs.default.name", hdfsroot);
      FileSystem fs = FileSystem.get(conf);
      Path tablePath = getTablePath(dt);
      Path filePath = new Path(tablePath, getTableName(dt));
      fs.mkdirs(tablePath);
      SequenceFile.Writer w = SequenceFile.createWriter(fs, conf,
        filePath, LongWritable.class, cls);

      int cnt = 0;
      for (String tmp : data) {
        record.parse(tmp + "\n");
        w.append(new LongWritable(cnt), record);
      }

      w.close();
    } catch (ClassNotFoundException cnfe) {
     throw new IOException(cnfe);
    } catch (RecordParser.ParseError pe) {
     throw new IOException(pe);
    }
  }

  @Override
  public void createFile(DATATYPES dt, String data) throws Exception {
    createFile(dt, new String[] { data });
  }

  public String[] codeGen(DATATYPES dt) throws Exception {

    CodeGenTool codeGen = new CodeGenTool();

    String[] codeGenArgs = getCodeGenArgv(dt);
    SqoopOptions options = codeGen.parseArguments(codeGenArgs, null, null,
      true);
    String username = MSSQLTestUtils.getDBUserName();
    String password = MSSQLTestUtils.getDBPassWord();

    options.setUsername(username);
    options.setPassword(password);
    codeGen.validateOptions(options);

    int ret = codeGen.run(options);
    assertEquals(0, ret);
    List<String> generatedJars = codeGen.getGeneratedJarFiles();

    assertNotNull(generatedJars);
    assertEquals("Expected 1 generated jar file", 1, generatedJars.size());
    String jarFileName = generatedJars.get(0);
    // Sqoop generates jars named "foo.jar"; by default, this should contain
    // a class named 'foo'. Extract the class name.
    Path jarPath = new Path(jarFileName);
    String jarBaseName = jarPath.getName();
    assertTrue(jarBaseName.endsWith(".jar"));
    assertTrue(jarBaseName.length() > ".jar".length());
    String className = jarBaseName.substring(0, jarBaseName.length()
      - ".jar".length());

    LOG.info("Using jar filename: " + jarFileName);
    LOG.info("Using class name: " + className);

    ClassLoader prevClassLoader = null;


    if (null != jarFileName) {
    prevClassLoader = ClassLoaderStack.addJarFile(jarFileName,
      className);
    System.out.println("Jar,class =" + jarFileName + " , "
      + className);
    }

    // Now run and verify the export.
    LOG.info("Exporting SequenceFile-based data");
    jars.put(dt, jarFileName);
    return (getArgv(dt, "--class-name", className, "--jar-file",
     jarFileName));



  }

  protected String[] getArgv(DATATYPES dt) {

    String[] args = super.getArgv(dt);
    String[] addtionalArgs = Arrays.copyOf(args, args.length + 4);

    String[] additional = new String[4];
    additional[0] = "--class-name";
    additional[1] = getTableName(dt);
    additional[2] = "--jar-file";
    additional[3] = jars.get(dt).toString();
    for (int i = args.length, j = 0; i < addtionalArgs.length; i++, j++) {
     addtionalArgs[i] = additional[j];
    }

    for (String a : addtionalArgs) {
     System.out.println(a);
    }
    return addtionalArgs;
  }

  /**
  * @return an argv for the CodeGenTool to use when creating tables to
  *         export.
  */
  protected String[] getCodeGenArgv(DATATYPES dt) {
    List<String> codeGenArgv = new ArrayList<String>();

    codeGenArgv.add("--table");
    codeGenArgv.add(getTableName(dt));
    codeGenArgv.add("--connect");
    codeGenArgv.add(getConnectString());
    codeGenArgv.add("--fields-terminated-by");
    codeGenArgv.add("\\t");
    codeGenArgv.add("--lines-terminated-by");
    codeGenArgv.add("\\n");

    return codeGenArgv.toArray(new String[0]);
  }

  /**
  * Create the argv to pass to Sqoop.
  *
  * @param includeHadoopFlags
  *            if true, then include -D various.settings=values
  * @param rowsPerStmt
  *            number of rows to export in a single INSERT statement.
  * @param statementsPerTx
  *            ## of statements to use in a transaction.
  * @return the argv as an array of strings.
  */
  protected String[] getArgv(DATATYPES dt, String... additionalArgv) {
    ArrayList<String> args = new ArrayList<String>();

    // Any additional Hadoop flags (-D foo=bar) are prepended.
    if (null != additionalArgv) {
      boolean prevIsFlag = false;
      for (String arg : additionalArgv) {
        if (arg.equals("-D")) {
          args.add(arg);
          prevIsFlag = true;
        } else if (prevIsFlag) {
          args.add(arg);
          prevIsFlag = false;
        }
      }
    }

    // The sqoop-specific additional args are then added.
    if (null != additionalArgv) {
      boolean prevIsFlag = false;
      for (String arg : additionalArgv) {
        if (arg.equals("-D")) {
          prevIsFlag = true;
          continue;
        } else if (prevIsFlag) {
          prevIsFlag = false;
          continue;
        } else {
         // normal argument.
          args.add(arg);
        }
      }
    }

    args.add("--table");
    args.add(getTableName(dt));
    args.add("--export-dir");
    args.add(getTablePath(dt).toString());
    args.add("--connect");
    args.add(getConnectString());
    args.add("--fields-terminated-by");
    args.add("\\t");
    args.add("--lines-terminated-by");
    args.add("\\n");
    args.add("-m");
    args.add("1");

    LOG.debug("args:");
    for (String a : args) {
     LOG.debug("  " + a);
    }

    return args.toArray(new String[0]);
  }

  public String getOutputFileName() {
    return "ManagerCompatExportSeq.txt";
  }

}
