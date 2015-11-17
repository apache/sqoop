/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.submission.spark;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.spark.JobExecutionStatus;
import org.apache.spark.SparkException;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.driver.JobManager;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.driver.SubmissionEngine;
import org.apache.sqoop.error.code.MapreduceSubmissionError;
import org.apache.sqoop.error.code.SparkSubmissionError;
import org.apache.sqoop.execution.spark.SparkExecutionEngine;
import org.apache.sqoop.execution.spark.SparkJobRequest;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.SubmissionError;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counters;

/**
 * This is very simple and straightforward implementation of spark submission
 * engine.
 */
public class SparkSubmissionEngine extends SubmissionEngine {

    private static Logger LOG = Logger.getLogger(SparkSubmissionEngine.class);

    // yarn config from yarn-site.xml

    // private Configuration yarnConfiguration;

    private SqoopSparkClient sparkClient;

    private SqoopConf sqoopConf;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(MapContext context, String prefix) {
        LOG.info("Initializing Spark Submission Engine");

        sqoopConf = new SqoopConf();
        //TODO: Load configured spark configuration directory
        //TODO: Create spark client, for now a local one
        try {

            sqoopConf.add(Constants.SPARK_UI_ENABLED, "false");
            sqoopConf.add(Constants.SPARK_DRIVER_ALLOWMULTIPLECONTEXTS, "true");

            sparkClient = SqoopSparkClientFactory.createSqoopSparkClient(sqoopConf);
        } catch (IOException | SparkException e) {
            throw new SqoopException(SparkSubmissionError.SPARK_0002, e);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        super.destroy();
        LOG.info("Destroying Spark Submission Engine");

        // Closing spark client
        try {
            sparkClient.close();
        } catch (IOException e) {
            throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0005, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isExecutionEngineSupported(Class<?> executionEngineClass) {
        return executionEngineClass == SparkExecutionEngine.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean submit(JobRequest jobRequest) {
        assert jobRequest instanceof SparkJobRequest;

        // We're supporting only map reduce jobs
        SparkJobRequest request = (SparkJobRequest) jobRequest;

        //TODO: Review SPARK_MARTER variable
        //sqoopConf.add(Constants.SPARK_MASTER, "local");// + request.getExtractors() + "]");

        //setYarnConfig(request);

        try {
            sparkClient.execute(jobRequest);
            request.getJobSubmission().setExternalJobId(jobRequest.getJobName());
            request.getJobSubmission().setProgress(progress(request));
            SubmissionStatus successful=convertSparkState(sparkClient.getSparkContext().statusTracker()
                    .getJobInfo(sparkClient.getSparkContext().sc().jobProgressListener().jobIdToData().size()-1)
                    .status());
            if (successful==SubmissionStatus.SUCCEEDED) {
                request.getJobSubmission().setStatus(SubmissionStatus.SUCCEEDED);
            } else {
                // treat any other state as failed
                request.getJobSubmission().setStatus(SubmissionStatus.FAILED);
            }

            // there is no failure info in this job api, unlike the running job
            request.getJobSubmission().setError(null);
            request.getJobSubmission().setLastUpdateDate(new Date());



        } catch (Exception e) {
            SubmissionError error = new SubmissionError();
            error.setErrorSummary(e.toString());
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            writer.flush();
            error.setErrorDetails(writer.toString());

            request.getJobSubmission().setError(error);
            LOG.error("Error in submitting job", e);
            return false;
        }

        return true;

    }

    //TODO: Review how to stop a Spark job


    /**
     * {@inheritDoc}
     */
    //    @Override
    public void stop(String jobId) {

        LOG.info("Destroying Spark Submission Engine");
        try {
            sparkClient.stop(JobManager.getInstance().status(jobId).getExternalJobId());
        } catch (Exception e) {
            throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(MSubmission submission) {
        double progress = -1;
        Counters counters = null;
        String externalJobId = submission.getExternalJobId();
        try {


            // these properties change as the job runs, rest of the submission attributes
            // do not change as job runs
            SubmissionStatus newStatus = convertSparkState(sparkClient.getSparkContext().statusTracker()
                    .getJobInfo(sparkClient.getSparkContext().sc().jobProgressListener().jobIdToData().size()-1)
                    .status());
            if (newStatus.isRunning()) {
                progress = (Double.valueOf(sqoopConf.get("mapTime"))+Double.valueOf(sqoopConf.get
                        ("reduceTime")))/2;
            }
//            else {
//                counters = counters(sparkClient.getSparkContext().sc().jobProgressListener().);
//            }
            submission.setStatus(newStatus);
//            submission.setCounters(counters);
            submission.setProgress(progress);
            submission.setLastUpdateDate(new Date());
        } catch (Exception e) {
            throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
        }
        // not much can be done, since we do not have easy api to ping spark for
        // app stats from in process
    }

    /**
     * Convert spark specific job status constants to Sqoop job status
     * constants.
     *
     * @param status Spark job constant
     * @return Equivalent submission status
     */
    private SubmissionStatus convertSparkState(JobExecutionStatus status) {
        if (JobExecutionStatus.RUNNING == status) {
            return SubmissionStatus.RUNNING;
        } else if (JobExecutionStatus.FAILED == status) {
            return SubmissionStatus.FAILED;
        } else if (JobExecutionStatus.UNKNOWN == status) {
            return SubmissionStatus.UNKNOWN;
        } else if (JobExecutionStatus.SUCCEEDED == status) {
            return SubmissionStatus.SUCCEEDED;
        }
        throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0004,
                "Unknown status " + status);
    }

    private double progress(SparkJobRequest sparkJobRequest) {
        try {
            if (sparkClient.getSparkContext().statusTracker()
                    .getJobInfo(sparkClient.getSparkContext().sc().jobProgressListener().jobIdToData().size()-1)
                    .status()== null) {
                // Return default value
                return -1;

            }
            return (Double.valueOf(sparkJobRequest.getConf().get("mapTime"))+Double.valueOf(sparkJobRequest.getConf()
                    .get("reduceTime")))/2;
            //return sparkClient.getSparkContext().sc().jobProgressListener().activeJobs();
//            return System.currentTimeMillis()-sparkClient.getSparkContext().sc().jobProgressListener().startTime();

//            return JobManager.getInstance().status(jobId).getProgress();
        } catch (Exception e) {
            throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
        }
    }
    /**
     * Detect MapReduce local mode.
     *
     * @return True if we're running in local mode
     */
    private boolean isLocal() {
        if (sparkClient.getSparkConf().get(Constants.SPARK_MASTER).startsWith("yarn"))
            return false;
        return true;
    }

}
