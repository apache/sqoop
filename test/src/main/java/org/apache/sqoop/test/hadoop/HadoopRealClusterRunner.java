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

package org.apache.sqoop.test.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/*
* This class enables running tests against a real cluster.
* From the command line, it expects "sqoop.hadoop.config.path"
* variable to point to directory containing cluster config files,
* otherwise it tries loading from default location: /etc/hadoop/conf.
*/
public class HadoopRealClusterRunner extends HadoopRunner {

  private static final Logger LOG = Logger
      .getLogger(HadoopRealClusterRunner.class);

  /*
  * This method loads config files for real cluster.
  * core-site.xml, mapred-site.xml and hdfs-site.xml are mandatory
  * while yarn-site.xml is optional to let tests execute against old
  * (non-yarn based) M/R clusters.
   */
  @Override
  public Configuration prepareConfiguration(Configuration config)
      throws Exception {
    String configPath = System.getProperty(
        "sqoop.hadoop.config.path", "/etc/hadoop/conf");
    LOG.debug("Config path is: " + configPath);

    File dir = new File(configPath);
    String [] files = dir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("-site.xml");
      }
    });

    if(files == null) {
      throw new FileNotFoundException("Hadoop config files not found: " + configPath);
    }

    // Add each config file to configuration object
    for (String file : files) {
      LOG.info("Found hadoop configuration file " + file);
      config.addResource(new Path(configPath, file));
    }
    return config;
  }

  @Override
  public void start() throws Exception {
    // Do nothing

  }

  @Override
  public void stop() throws Exception {
    // Do nothing

  }

}
