package org.apache.sqoop.execution.spark;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.driver.ExecutionEngine;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.job.SparkJobConstants;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;

public class SparkExecutionEngine extends ExecutionEngine {

    private static Logger LOG = Logger.getLogger(SparkExecutionEngine.class);

    /**
     *  {@inheritDoc}
     */
    @Override
    public JobRequest createJobRequest() {
        return new SparkJobRequest();
    }

    @Override
    public void prepareJob(JobRequest jobRequest) {
        assert jobRequest instanceof SparkJobRequest;
        SparkJobRequest sparkJobRequest = (SparkJobRequest) jobRequest;

        // Add jar dependencies
        addDependencies(sparkJobRequest);
/**
        // Configure map-reduce classes for import
        sparkJobRequest.setInputFormatClass(SqoopInputFormat.class);

        sparkJobRequest.setMapperClass(SqoopMapper.class);
        sparkJobRequest.setMapOutputKeyClass(SqoopWritable.class);
        sparkJobRequest.setMapOutputValueClass(NullWritable.class);

        sparkJobRequest.setOutputFormatClass(SqoopNullOutputFormat.class);
        sparkJobRequest.setOutputKeyClass(SqoopWritable.class);
        sparkJobRequest.setOutputValueClass(NullWritable.class);
*/

        From from = (From) sparkJobRequest.getFrom();
        To to = (To) sparkJobRequest.getTo();
        MutableMapContext context = sparkJobRequest.getDriverContext();

        context.setString(SparkJobConstants.SUBMITTING_USER, jobRequest.getJobSubmission().getCreationUser());
        context.setString(SparkJobConstants.JOB_ETL_PARTITIONER, from.getPartitioner().getName());
        context.setString(SparkJobConstants.JOB_ETL_PARTITION, from.getPartition().getName());

        context.setString(SparkJobConstants.JOB_ETL_EXTRACTOR, from.getExtractor().getName());
        context.setString(SparkJobConstants.JOB_ETL_LOADER, to.getLoader().getName());
        context
                .setString(SparkJobConstants.JOB_ETL_FROM_DESTROYER, from.getDestroyer().getName());
        context.setString(SparkJobConstants.JOB_ETL_TO_DESTROYER, to.getDestroyer().getName());
        context.setString(SparkJobConstants.FROM_INTERMEDIATE_DATA_FORMAT, sparkJobRequest
                .getIntermediateDataFormat(Direction.FROM).getName());
        context.setString(SparkJobConstants.TO_INTERMEDIATE_DATA_FORMAT, sparkJobRequest
                .getIntermediateDataFormat(Direction.TO).getName());

        if (sparkJobRequest.getExtractors() != null) {
            context.setInteger(SparkJobConstants.JOB_ETL_EXTRACTOR_NUM,
                    sparkJobRequest.getExtractors());

            LOG.debug("Configured Extractor size:" + sparkJobRequest.getExtractors());

        }


        for (Map.Entry<String, String> entry : jobRequest.getDriverContext()) {
            if (entry.getValue() == null) {
                LOG.warn("Ignoring null driver context value for key " + entry.getKey());
                continue;
            }
            sparkJobRequest.getConf().put(entry.getKey(), entry.getValue());
        }

        // Serialize connector context as a sub namespace
        for (Map.Entry<String, String> entry : jobRequest.getConnectorContext(Direction.FROM)) {
            if (entry.getValue() == null) {
                LOG.warn("Ignoring null connector context value for key " + entry.getKey());
                continue;
            }
            sparkJobRequest.getConf().put(SparkJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT + entry.getKey(),
                    entry.getValue());
        }

        for (Map.Entry<String, String> entry : jobRequest.getConnectorContext(Direction.TO)) {
            if (entry.getValue() == null) {
                LOG.warn("Ignoring null connector context value for key " + entry.getKey());
                continue;
            }
            sparkJobRequest.getConf().put(SparkJobConstants.PREFIX_CONNECTOR_TO_CONTEXT + entry.getKey(),
                    entry.getValue());
        }

        for (Map.Entry<String, String> entry : jobRequest.getDriverContext()) {
            if (entry.getValue() == null) {
                LOG.warn("Ignoring null connector context value for key " + entry.getKey());
                continue;
            }
            sparkJobRequest.getConf().put(SparkJobConstants.PREFIX_CONNECTOR_DRIVER_CONTEXT + entry.getKey(),
                    entry.getValue());
        }

        /**/

    }

    /**
     * If the execution engine has additional dependencies that needs to be available execution job time. This method will register all dependencies in the request object.
     *
     * @param jobrequest Active job request object.
     */
    protected void addDependencies(SparkJobRequest jobrequest) {
        // not much to do here
    }
}
