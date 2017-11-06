/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.infrastructure.kerberos;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class MiniKdcInfrastructureRule implements TestRule, MiniKdcInfrastructure {

  private MiniKdc miniKdc;

  private Properties configuration;

  private File workDir;

  private String testPrincipal;

  private File keytabFile;

  public MiniKdcInfrastructureRule() {
    File baseDir = Files.createTempDir();
    this.workDir = new File(baseDir, "MiniKdcWorkDir");
    this.configuration = MiniKdc.createConf();
  }

  @Override
  public void start() {
    try {
      miniKdc = new MiniKdc(configuration, workDir);
      miniKdc.start();
      createTestPrincipal();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void createTestPrincipal() {
    try {
      createKeytabFile();
      testPrincipal = currentUser() + "/" + miniKdc.getHost();
      miniKdc.createPrincipal(keytabFile, testPrincipal);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void createKeytabFile() {
    try {
      keytabFile = new File(workDir.getAbsolutePath(), "keytab");
      keytabFile.createNewFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    try {
      miniKdc.stop();
      FileUtils.deleteDirectory(workDir);
      configuration = null;
      miniKdc = null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        start();
        base.evaluate();
        stop();
      }
    };
  }

  @Override
  public String getTestPrincipal() {
    return testPrincipal;
  }

  @Override
  public String getRealm() {
    return miniKdc.getRealm();
  }

  @Override
  public String getKeytabFilePath() {
    return keytabFile.getAbsolutePath();
  }

  private String currentUser() {
    return System.getProperty("user.name");
  }
}
