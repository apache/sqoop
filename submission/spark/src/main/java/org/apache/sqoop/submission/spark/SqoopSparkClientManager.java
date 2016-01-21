package org.apache.sqoop.submission.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sqoop.driver.JobRequest;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

public abstract class SqoopSparkClientManager implements SqoopSparkClient {

    protected static final Splitter CSV_SPLITTER = Splitter.on(",").omitEmptyStrings();

    protected JavaSparkContext context;

    protected List<String> localJars = new ArrayList<String>();

    protected List<String> localFiles = new ArrayList<String>();

    @Override public void execute(JobRequest request) throws Exception {

    }

    public SparkConf getSparkConf() {
        return context.getConf();
    }

    public int getExecutorCount() {
        return context.sc().getExecutorMemoryStatus().size();
    }

    public int getDefaultParallelism() throws Exception {
        return context.sc().defaultParallelism();
    }


    public void addResources(String addedFiles) {
        for (String addedFile : CSV_SPLITTER.split(Strings.nullToEmpty(addedFiles))) {
            if (!localFiles.contains(addedFile)) {
                localFiles.add(addedFile);
                context.addFile(addedFile);
            }
        }
    }

    private void addJars(String addedJars) {
        for (String addedJar : CSV_SPLITTER.split(Strings.nullToEmpty(addedJars))) {
            if (!localJars.contains(addedJar)) {
                localJars.add(addedJar);
                context.addJar(addedJar);
            }
        }
    }

}
