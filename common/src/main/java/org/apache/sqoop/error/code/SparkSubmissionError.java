package org.apache.sqoop.error.code;

import org.apache.sqoop.common.ErrorCode;

/**
 *
 */
public enum SparkSubmissionError implements ErrorCode {

    SPARK_0001("Unknown error"),

    SPARK_0002("Failure on submission engine initialization"),

    SPARK_0003("Can't get RunningJob instance"),

    SPARK_0004("Unknown spark job status"),

    SPARK_0005("Failure on submission engine destroy"),

    ;

    private final String message;

    private SparkSubmissionError(String message) {
        this.message = message;
    }

    public String getCode() {
        return name();
    }

    public String getMessage() {
        return message;
    }
}