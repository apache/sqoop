package org.apache.sqoop.execution.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.driver.JobRequest;

/**
 * Custom data required for the spark job request
 */
public class SparkJobRequest extends JobRequest implements Serializable {

    private static final long serialVersionUID = 1L;
    private Map<String, String> map;
    private List<IntermediateDataFormat<?>> rowData = new ArrayList<IntermediateDataFormat<?>>();

    /**
     * Map-reduce specific options.
     */
    Class<? extends InputFormat> inputFormatClass;
    Class<? extends Mapper> mapperClass;
    Class<? extends Writable> mapOutputKeyClass;
    Class<? extends Writable> mapOutputValueClass;
    Class<? extends OutputFormat> outputFormatClass;
    Class<? extends Writable> outputKeyClass;
    Class<? extends Writable> outputValueClass;

    public SparkJobRequest() {
        super();
        map = new HashMap<String, String>();
    }

    public Class<? extends InputFormat> getInputFormatClass() {
        return inputFormatClass;
    }

    public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
        this.inputFormatClass = inputFormatClass;
    }

    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }

    public void setMapperClass(Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass;
    }

    public Class<? extends Writable> getMapOutputKeyClass() {
        return mapOutputKeyClass;
    }

    public void setMapOutputKeyClass(Class<? extends Writable> mapOutputKeyClass) {
        this.mapOutputKeyClass = mapOutputKeyClass;
    }

    public Class<? extends Writable> getMapOutputValueClass() {
        return mapOutputValueClass;
    }

    public void setMapOutputValueClass(Class<? extends Writable> mapOutputValueClass) {
        this.mapOutputValueClass = mapOutputValueClass;
    }

    public Class<? extends OutputFormat> getOutputFormatClass() {
        return outputFormatClass;
    }

    public void setOutputFormatClass(Class<? extends OutputFormat> outputFormatClass) {
        this.outputFormatClass = outputFormatClass;
    }

    public Class<? extends Writable> getOutputKeyClass() {
        return outputKeyClass;
    }

    public void setOutputKeyClass(Class<? extends Writable> outputKeyClass) {
        this.outputKeyClass = outputKeyClass;
    }

    public Class<? extends Writable> getOutputValueClass() {
        return outputValueClass;
    }

    public void setOutputValueClass(Class<? extends Writable> outputValueClass) {
        this.outputValueClass = outputValueClass;
    }

    public Map<String, String> getConf() {
        return map;
    }

    public void addData(IntermediateDataFormat<?> idf) {
        rowData.add(idf);
    }
    public List<IntermediateDataFormat<?>> getData() {
        return rowData;
    }

}
