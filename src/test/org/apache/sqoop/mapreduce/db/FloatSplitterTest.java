package org.apache.sqoop.mapreduce.db;

import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FloatSplitterTest {

    @Test
    public void split() {
        double MIN_INCREMENT = 10000 * Double.MIN_VALUE;

        System.out.println("Generating splits for a floating-point index column. Due to the");
        System.out.println("imprecise representation of floating-point values in Java, this");
        System.out.println("may result in an incomplete import.");
        System.out.println("You are strongly encouraged to choose an integral split column.");

        List<InputSplit> splits = new ArrayList<InputSplit>();
        String colName = "float_code";
        double minVal = 1.111;
        double maxVal = 133.333;

        // Use this as a hint. May need an extra task if the size doesn't
        // divide cleanly.
        int numSplits = 2;
        double splitSize = (maxVal - minVal) / (double) numSplits;

        if (splitSize < MIN_INCREMENT) {
            splitSize = MIN_INCREMENT;
        }

        String lowClausePrefix = colName + " >= ";
        String highClausePrefix = colName + " < ";

        double curLower = minVal;
        double curUpper = curLower + splitSize;

        while (curUpper < maxVal) {
            splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                    lowClausePrefix + Double.toString(curLower),
                    highClausePrefix + Double.toString(curUpper)));

            curLower = curUpper;
            curUpper += splitSize;
        }

        // Catch any overage and create the closed interval for the last split.
        if (curLower <= maxVal || splits.size() == 1) {
            splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
//                    lowClausePrefix + Double.toString(curUpper),
                    lowClausePrefix + Double.toString(curLower),
                    colName + " <= " + Double.toString(maxVal)));
        }

        System.out.println(splits);
    }
}