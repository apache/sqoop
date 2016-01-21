package org.apache.sqoop.submission.spark;

import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.connector.matcher.MatcherFactory;
import org.apache.sqoop.error.code.SparkExecutionError;
import org.apache.sqoop.execution.spark.SparkJobRequest;
import org.apache.sqoop.job.SparkJobConstants;
import org.apache.sqoop.job.SparkPrefixContext;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;

public class SqoopExtractFunction implements Function<Partition, List<IntermediateDataFormat<?>>>,
        Serializable {
    private SparkJobRequest req;
    public static final Logger LOG = Logger.getLogger(SqoopExtractFunction.class);

    public SqoopExtractFunction(SparkJobRequest request) {
        req = request;
    }

    @Override
    public List<IntermediateDataFormat<?>> call(Partition p) throws Exception {

        long mapTime = System.currentTimeMillis();
        String extractorName = req.getDriverContext().getString(SparkJobConstants.JOB_ETL_EXTRACTOR);

        Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

        Schema fromSchema = req.getJobSubmission().getFromSchema();

        Schema toSchema = req.getJobSubmission().getToSchema();

        Matcher matcher = MatcherFactory.getMatcher(fromSchema, toSchema);

        String fromIDFClass = req.getDriverContext().getString(
                SparkJobConstants.FROM_INTERMEDIATE_DATA_FORMAT);
        IntermediateDataFormat<Object> fromIDF = (IntermediateDataFormat<Object>) ClassUtils
                .instantiate(fromIDFClass);
        fromIDF.setSchema(matcher.getFromSchema());

        String toIDFClass = req.getDriverContext().getString(
                SparkJobConstants.TO_INTERMEDIATE_DATA_FORMAT);
        IntermediateDataFormat<Object> toIDF = (IntermediateDataFormat<Object>) ClassUtils
                .instantiate(toIDFClass);
        toIDF.setSchema(matcher.getToSchema());

        // Objects that should be passed to the Executor execution
        SparkPrefixContext subContext = new SparkPrefixContext(req.getConf(),
                SparkJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);

        Object fromLinkConfig = req.getConnectorLinkConfig(Direction.FROM);
        Object fromJobConfig = req.getJobConfig(Direction.FROM);

        ExtractorContext extractorContext = new ExtractorContext(subContext, new SparkDataWriter(
                req, fromIDF, toIDF, matcher), fromSchema, SparkJobConstants.SUBMITTING_USER);

        try {
            LOG.info("Starting extractor... ");
            extractor.extract(extractorContext, fromLinkConfig, fromJobConfig, p);
        } catch (Exception e) {
            throw new SqoopException(SparkExecutionError.SPARK_EXEC_0000, e);
        } finally {
            LOG.info("Stopping extractor service");
        }

        LOG.info("Extractor has finished");
        LOG.info(">>> MAP time ms:" + (System.currentTimeMillis() - mapTime));
        req.getConf().put("mapTime",""+(System.currentTimeMillis() - mapTime));

        return req.getData();
    }

}
