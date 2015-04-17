/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.sqoop.common.test.kafka;

import java.io.IOException;

/**
 * This class encapsulates logic behind which Kafka class to instantiate
 */
public class KafkaRunnerFactory {

  private static final String KAFKA_CLASS_PROPERTY = "sqoop.kafka.runner.class";

  public static KafkaRunnerBase getKafkaRunner() throws ClassNotFoundException, IllegalAccessException,
                                                        InstantiationException, InterruptedException, IOException {
    String className = System.getProperty(KAFKA_CLASS_PROPERTY);
    if(className == null) {
      return new KafkaLocalRunner();
    } else {
      Class<?> klass = Class.forName(className);
      return (KafkaRunnerBase) klass.newInstance();
    }
  }

}
