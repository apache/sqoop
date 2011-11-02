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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ManagerFactory;

/**
 * An external ConnFactory used by ExtConnFactoryTest.
 */
public class ExtFactory extends ManagerFactory {
  public static final Log LOG =
      LogFactory.getLog(ExtFactory.class.getName());

  public ExtFactory() {
    LOG.info("Instantiating ExtFactory");
  }

  public ConnManager accept(SqoopOptions options) {
    LOG.info("ExtFactory accepting manager for: "
        + options.getConnectString());
    return new ExtConnFactoryTest.FailingManager(options);
  }
}
