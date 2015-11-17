package org.apache.sqoop.submission.spark;

public interface SqoopSparkJobRef {

    String getJobId();

    SqoopSparkJobStatus getSparkJobStatus();

    boolean cancelJob();

}