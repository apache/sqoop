package org.apache.sqoop.submission.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.compress.utils.CharsetNames;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;

public class SqoopSparkClientFactory {

    protected static final transient Log LOG = LogFactory.getLog(SqoopSparkClientFactory.class);
    private static final String SPARK_DEFAULT_CONF_FILE = "conf/spark-defaults.conf";
    private static final String SPARK_DEFAULT_MASTER = "local";
    private static final String SPARK_DEFAULT_APP_NAME = "sqoop-spark";
    private static final String SPARK_DEFAULT_SERIALIZER = "org.apache.spark.serializer.KryoSerializer";


    public static SqoopSparkClientManager createSqoopSparkClient(SqoopConf sqoopConf)
            throws IOException, SparkException {

        Map<String, String> sparkConf = prepareSparkConfMapFromSqoopConfig(sqoopConf);
        // Submit spark job through local spark context while spark master is local
        // mode, otherwise submit spark job through remote spark context.
        String master = sparkConf.get(Constants.SPARK_MASTER);
        if (master.equals("local") || master.startsWith("local[")) {
            // With local spark context, all user sessions share the same spark context.
            return LocalSqoopSparkClient.getInstance(generateSparkConf(sparkConf));
        } else {
            LOG.info("Using yarn submitter");
            //TODO: hook up yarn submitter
            return null;
        }
//        if (master.equals("yarn") || master.startsWith("yarn")) {
//
//            LOG.info("Using yarn submitter");
//            return YarnSqoopSparkClient.getInstance(sparkConf);
//
//        } else if (master.equals("local") || master.startsWith("local[")) {
//            // With local spark context, all user sessions share the same spark context.
//            return LocalSqoopSparkClient.getInstance(generateSparkConf(sparkConf));
//
//        } else {
//            LOG.error("Unable to parse master configuration: "+ master);
//            return null;
//        }
    }

    public static Map<String, String> prepareSparkConfMapFromSqoopConfig(SqoopConf sqoopConf) {
        Map<String, String> sparkConf = new HashMap<String, String>();
        // set default spark configurations.
        sparkConf.put(Constants.SPARK_MASTER, SPARK_DEFAULT_MASTER);
        sparkConf.put(Constants.SPARK_APP_NAME, SPARK_DEFAULT_APP_NAME);
        sparkConf.put(Constants.SPARK_SERIALIZER, SPARK_DEFAULT_SERIALIZER);

        for(Map.Entry<String, String> p :sqoopConf.getProps().entrySet()){
            LOG.info("sqoop spark properties from: " + p.getKey() + ": " + p.getValue());
        }
        // load properties from spark-defaults.conf.
        InputStream inputStream = null;
        try {
            inputStream = SqoopSparkClientFactory.class.getClassLoader().getResourceAsStream(
                    SPARK_DEFAULT_CONF_FILE);
            if (inputStream != null) {
                LOG.info("Loading spark properties from:" + SPARK_DEFAULT_CONF_FILE);
                Properties properties = new Properties();
                properties.load(new InputStreamReader(inputStream, CharsetNames.UTF_8));
                for (String propertyName : properties.stringPropertyNames()) {
                    if (propertyName.startsWith("spark")) {
                        String value = properties.getProperty(propertyName);
                        sparkConf.put(propertyName, properties.getProperty(propertyName));
                        LOG.info(String.format("Load spark property from %s (%s -> %s).",
                                SPARK_DEFAULT_CONF_FILE, propertyName, value));
                    }
                }
            }
        } catch (IOException e) {
            LOG.info("Failed to open spark configuration file:" + SPARK_DEFAULT_CONF_FILE, e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    LOG.debug("Failed to close inputstream.", e);
                }
            }
        }
        return sparkConf;
    }

    static SparkConf generateSparkConf(Map<String, String> conf) {
        SparkConf sparkConf = new SparkConf(false);
        for (Map.Entry<String, String> entry : conf.entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
        }
        return sparkConf;
    }

}
