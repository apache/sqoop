package org.apache.sqoop.mapreduce.db;

import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestBooleanSplitter {

    @Test
    public void split() {
        List<InputSplit> splits = new ArrayList<>();
        String colName = "isCheck";

        boolean minVal = false;
        boolean maxVal = true;

        // Use one or two splits.
        if (!minVal) {
            splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                    colName + " = FALSE", colName + " = FALSE"));
        }

        if (maxVal) {
            splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                    colName + " = TRUE", colName + " = TRUE"));
        }

        System.out.println(splits);
    }
}