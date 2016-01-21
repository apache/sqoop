package org.apache.sqoop.submission.spark;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.error.code.SparkExecutionError;
import org.apache.sqoop.execution.spark.SparkJobRequest;
import org.apache.sqoop.job.SparkJobConstants;
import org.apache.sqoop.job.SparkPrefixContext;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;

public class SqoopSparkDriver {

    public static final String DEFAULT_EXTRACTORS = "defaultExtractors";
    public static final String NUM_LOADERS = "numLoaders";

    private static final Log LOG = LogFactory.getLog(SqoopSparkDriver.class.getName());

    public static void execute(JobRequest request, SparkConf conf, JavaSparkContext sc)
            throws Exception {
        assert request instanceof SparkJobRequest;
        SparkJobRequest sparkJobRequest = (SparkJobRequest) request;

        LOG.info("Executing sqoop spark job");

        long totalTime = System.currentTimeMillis();
        SparkPrefixContext driverContext = new SparkPrefixContext(sparkJobRequest.getConf(),
                SparkJobConstants.PREFIX_CONNECTOR_DRIVER_CONTEXT);

        int defaultExtractors = conf.getInt(DEFAULT_EXTRACTORS, 10);
        long numExtractors = (driverContext.getLong(SparkJobConstants.JOB_ETL_EXTRACTOR_NUM,
                defaultExtractors));
        int numLoaders = conf.getInt(NUM_LOADERS, 1);

        List<Partition> sp = getPartitions(sparkJobRequest, numExtractors);
        LOG.info(">>> Partition size:" + sp.size());

        JavaRDD<Partition> rdd = sc.parallelize(sp, sp.size());

        SqoopExtractFunction extractFunction= new SqoopExtractFunction(sparkJobRequest);
        JavaRDD<List<IntermediateDataFormat<?>>> mapRDD = rdd.map(extractFunction);
              // if max loaders or num loaders is given repartition to adjust the max
        // loader parallelism
        if (numLoaders != numExtractors) {
            JavaRDD<List<IntermediateDataFormat<?>>> reParitionedRDD = mapRDD.repartition(numLoaders);
            LOG.info(">>> RePartition RDD size:" + reParitionedRDD.partitions().size());
            reParitionedRDD.mapPartitions(new SqoopLoadFunction(sparkJobRequest)).collect();
            //            sparkJobRequest.getJobSubmission().setStatus(SubmissionStatus.RUNNING);
        } else {
            LOG.info(">>> Mapped RDD size:" + mapRDD.partitions().size());
            mapRDD.mapPartitions(new SqoopLoadFunction(sparkJobRequest)).collect();
        }

        LOG.info(">>> TOTAL time ms:" + (System.currentTimeMillis() - totalTime));
        //Change status when job has finished
        //        sparkJobRequest.getJobSubmission().setStatus(SubmissionStatus.SUCCEEDED);
        LOG.info("Done EL in sqoop spark job, next call destroy apis");

    }

    @SuppressWarnings("unchecked")
    private static List<Partition> getPartitions(JobRequest request, long maxPartitions) {
        assert request instanceof SparkJobRequest;
        SparkJobRequest sparkJobRequest = (SparkJobRequest) request;
        String partitionerName = request.getDriverContext().getString(SparkJobConstants.JOB_ETL_PARTITIONER);
        @SuppressWarnings("rawtypes")
        Partitioner partitioner = (Partitioner) ClassUtils.instantiate(partitionerName);
        SparkPrefixContext context = new SparkPrefixContext(sparkJobRequest.getConf(),
                SparkJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);

        Object fromLinkConfig = request.getConnectorLinkConfig(Direction.FROM);
        Object fromJobConfig = request.getJobConfig(Direction.FROM);
        Schema fromSchema = request.getJobSubmission().getFromSchema();

        LOG.info(">>> Configured Partition size:" + maxPartitions);

        PartitionerContext partitionerContext = new PartitionerContext(context, maxPartitions,
                fromSchema, SparkJobConstants.SUBMITTING_USER);

        List<Partition> partitions = partitioner.getPartitions(partitionerContext, fromLinkConfig,
                fromJobConfig);

        if (partitions.size() > maxPartitions) {
            throw new SqoopException(SparkExecutionError.SPARK_EXEC_0000, String.format(
                    "Got %d, max was %d", partitions.size(), maxPartitions));
        }
        return partitions;
    }
}