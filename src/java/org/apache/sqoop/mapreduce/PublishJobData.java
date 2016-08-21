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
package org.apache.sqoop.mapreduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.SqoopJobDataPublisher;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.config.ConfigurationConstants;

import java.util.Date;

/**
 * Util class to publish job data to listeners.
 */
public final class PublishJobData {

    public static final Log LOG = LogFactory.getLog(PublishJobData.class.getName());

    private PublishJobData() {}

    public static void publishJobData(Configuration conf, SqoopOptions options,
                                      String operation, String tableName, long startTime) {
        // Publish metadata about export job to listeners (if they are registered with sqoop)
        long endTime = new Date().getTime();
        String publishClassName = conf.get(ConfigurationConstants.DATA_PUBLISH_CLASS);
        if (!StringUtils.isEmpty(publishClassName)) {
            try {
                Class publishClass =  Class.forName(publishClassName);
                Object obj = publishClass.newInstance();
                if (obj instanceof SqoopJobDataPublisher) {
                    SqoopJobDataPublisher publisher = (SqoopJobDataPublisher) obj;
                    SqoopJobDataPublisher.Data data =
                            new SqoopJobDataPublisher.Data(operation, options, tableName, startTime, endTime);
                    LOG.info("Published data is " + data.toString());
                    publisher.publish(data);
                } else {
                    LOG.warn("Publisher class not an instance of SqoopJobDataPublisher. Ignoring...");
                }
            } catch (Exception ex) {
                LOG.warn("Unable to publish " + operation + " data to publisher " + ex.getMessage(), ex);
            }
        }
    }
}
