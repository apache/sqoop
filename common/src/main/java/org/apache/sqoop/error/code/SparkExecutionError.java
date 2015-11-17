package org.apache.sqoop.error.code;


import org.apache.sqoop.common.ErrorCode;


public enum SparkExecutionError implements ErrorCode {

    SPARK_EXEC_0000("Unknown error");

    private final String message;

    SparkExecutionError(String message) {
        this.message = message;
    }

    public String getCode() {
        return name();
    }

    public String getMessage() {
        return message;
    }
}