package org.apache.sqoop.submission.spark;


import java.io.Closeable;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sqoop.driver.JobRequest;

public interface SqoopSparkClient extends Serializable,Closeable {

    void execute(JobRequest request) throws Exception;

    void stop(String jobId) throws Exception;


    /**
     * @return spark configuration
     */
    SparkConf getSparkConf();
    /**
     * @return spark configuration
     */
    JavaSparkContext getSparkContext();
    /**
     * @return the number of executors
     */
    int getExecutorCount() throws Exception;

    /**
     * For standalone mode, this can be used to get total number of cores.
     * @return  default parallelism.
     */
    int getDefaultParallelism() throws Exception;
}
