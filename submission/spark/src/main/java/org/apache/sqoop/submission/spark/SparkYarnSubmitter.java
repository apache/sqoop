package org.apache.sqoop.submission.spark;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

public class SparkYarnSubmitter {

    public static void submit(String[] args, SparkConf sparkConf) {
        YarnConfiguration yarnConfig = new YarnConfiguration();
        // convert the *- site xml to yarn conf
        ClientArguments cArgs = new ClientArguments(args, sparkConf);
        new Client(cArgs, yarnConfig, sparkConf).run();
    }

}
