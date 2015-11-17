package org.apache.sqoop.submission.spark;

public enum SparkJobState {

    SUBMITTED,
    INITING,
    RUNNING,
    SUCCEEDED,
    KILLED,
    FAILED,
    ERROR,
}

