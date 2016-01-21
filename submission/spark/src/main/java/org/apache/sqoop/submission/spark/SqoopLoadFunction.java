package org.apache.sqoop.submission.spark;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.connector.matcher.MatcherFactory;
import org.apache.sqoop.error.code.SparkExecutionError;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.execution.spark.SparkJobRequest;
import org.apache.sqoop.job.SparkJobConstants;
import org.apache.sqoop.job.SparkPrefixContext;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;

@SuppressWarnings("serial")
public class SqoopLoadFunction implements
        FlatMapFunction<Iterator<List<IntermediateDataFormat<?>>>, Void>, Serializable {

    private SparkJobRequest reqLoad;

    public static final Logger LOG = Logger.getLogger(SqoopLoadFunction.class);

    public SqoopLoadFunction(SparkJobRequest request) {
        reqLoad =  request;
    }

    @Override
    public Iterable<Void> call(Iterator<List<IntermediateDataFormat<?>>> data) throws Exception {

        long reduceTime = System.currentTimeMillis();

        String loaderName = reqLoad.getDriverContext().getString(SparkJobConstants.JOB_ETL_LOADER);
        Schema fromSchema = reqLoad.getJobSubmission().getFromSchema();
        Schema toSchema = reqLoad.getJobSubmission().getToSchema();
        Matcher matcher = MatcherFactory.getMatcher(fromSchema, toSchema);

        LOG.info("Sqoop Load Function is  starting");
        try {
            while (data.hasNext()) {
                DataReader reader = new SparkDataReader(data.next());

                Loader loader = (Loader) ClassUtils.instantiate(loaderName);

                SparkPrefixContext subContext = new SparkPrefixContext(reqLoad.getConf(),
                        SparkJobConstants.PREFIX_CONNECTOR_TO_CONTEXT);

                Object toLinkConfig = reqLoad.getConnectorLinkConfig(Direction.TO);
                Object toJobConfig = reqLoad.getJobConfig(Direction.TO);

                // Create loader context
                LoaderContext loaderContext = new LoaderContext(subContext, reader, matcher.getToSchema(),SparkJobConstants.SUBMITTING_USER);

                LOG.info("Running loader class " + loaderName);
                loader.load(loaderContext, toLinkConfig, toJobConfig);
                LOG.info("Loader has finished");
                LOG.info(">>> REDUCE time ms:" + (System.currentTimeMillis() - reduceTime));

                reqLoad.getConf().put("reduceTime",""+(System.currentTimeMillis() - reduceTime));
            }
        } catch (Throwable t) {
            LOG.error("Error while loading data out of MR job.", t);
            throw new SqoopException(SparkExecutionError.SPARK_EXEC_0000, t);
        }

        return Collections.singletonList(null);

    }

}
