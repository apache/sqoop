package org.apache.sqoop.submission.spark;

import org.apache.spark.JobExecutionStatus;
import org.apache.sqoop.common.SqoopException;

/**
 * SparkJobStatus identify what Hive want to know about the status of a Spark job.
 */
public interface SqoopSparkJobStatus {

    int getJobId();

    JobExecutionStatus getState() throws SqoopException;

    JobExecutionStatus getStatus() throws SqoopException;

    void cleanup();
}
