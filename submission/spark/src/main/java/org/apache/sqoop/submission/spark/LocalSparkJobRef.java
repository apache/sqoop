package org.apache.sqoop.submission.spark;

import org.apache.spark.api.java.JavaSparkContext;

public class LocalSparkJobRef implements SqoopSparkJobRef {

    private final String jobId;
    private final SqoopConf sqoopConf;
    private final LocalSparkJobStatus sparkJobStatus;
    private final JavaSparkContext javaSparkContext;

    public LocalSparkJobRef(String jobId, SqoopConf sqoopConf, LocalSparkJobStatus sparkJobStatus,
            JavaSparkContext javaSparkContext) {

        this.jobId = jobId;
        this.sqoopConf = sqoopConf;
        this.sparkJobStatus = sparkJobStatus;
        this.javaSparkContext = javaSparkContext;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public SqoopSparkJobStatus getSparkJobStatus() {
        return sparkJobStatus;
    }

    @Override
    public boolean cancelJob() {
        int id = Integer.parseInt(jobId);
        javaSparkContext.sc().cancelJob(id);
        return true;
    }

}
