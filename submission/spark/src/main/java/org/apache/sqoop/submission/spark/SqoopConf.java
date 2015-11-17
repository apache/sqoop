package org.apache.sqoop.submission.spark;

import java.util.HashMap;
import java.util.Map;


// contains all the required confs for sqoop-spark
public class SqoopConf {

    private Map<String, String> props = new HashMap<String, String>();

    SqoopConf() {

    }

    public Map<String, String> getProps() {
        return props;
    }

    public void add(String key, String value){
        props.put(key, value);
    }

    public String get(String key) {
        return props.get(key);
    }
}
