package org.apache.sqoop.submission.spark;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.error.code.MRExecutionError;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.execution.spark.SparkJobRequest;
import org.apache.sqoop.job.SparkJobConstants;
import org.apache.sqoop.utils.ClassUtils;

public class SparkDataWriter extends DataWriter {

    public static final Logger LOG = Logger.getLogger(SparkDataWriter.class);

    private SparkJobRequest request;

    private IntermediateDataFormat<Object> fromIDF;
    private IntermediateDataFormat<Object> toIDF;
    private Matcher matcher;

    public SparkDataWriter(JobRequest request, IntermediateDataFormat<Object> f,
            IntermediateDataFormat<Object> t, Matcher m) {
        assert request instanceof SparkJobRequest;

        this.request = (SparkJobRequest) request;
        fromIDF = f;
        toIDF = t;
        matcher = m;

    }

    @Override
    public void writeArrayRecord(Object[] array) {
        fromIDF.setObjectData(array);
        writeContent();
    }

    @Override
    public void writeStringRecord(String text) {
        fromIDF.setCSVTextData(text);
        writeContent();
    }

    @Override
    public void writeRecord(Object obj) {
        fromIDF.setData(obj);
        writeContent();
    }

    private void writeContent() {
        try {
            if (LOG.isDebugEnabled()) {
                //LOG.debug("Extracted data: " + fromIDF.getCSVTextData());
            }
            // NOTE: The fromIDF and the corresponding fromSchema is used only
            // for the matching process
            // The output of the mappers is finally written to the toIDF object
            // after the matching process
            // since the writable encapsulates the toIDF ==> new
            // SqoopWritable(toIDF)
            toIDF.setObjectData(matcher.getMatchingData(fromIDF.getObjectData()));
            // NOTE: We do not use the reducer to do the writing (a.k.a LOAD in
            // ETL).
            // Hence the mapper sets up the writable
            String toIDFClass = request.getDriverContext().getString(
                    SparkJobConstants.TO_INTERMEDIATE_DATA_FORMAT);
            IntermediateDataFormat<Object> newIDF = (IntermediateDataFormat<Object>) ClassUtils
                    .instantiate(toIDFClass);
            newIDF.setSchema(toIDF.getSchema());


            newIDF.setCSVTextData(toIDF.getCSVTextData());
            newIDF.setData(toIDF.getData());
            newIDF.setObjectData(toIDF.getObjectData());
            request.addData(newIDF);


        } catch (Exception e) {
            throw new SqoopException(MRExecutionError.MAPRED_EXEC_0013, e);
        }
    }
}

