package org.apache.sqoop.mapreduce.db;

import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DateSplitterTest {

    private OracleDateSplitter dateSplitter;
    private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Test
    public void split() throws Exception {
        dateSplitter = new OracleDateSplitter();
        String colName = "checkTime";
        final long MS_IN_SEC = 1000L;
        long minVal;
        long maxVal;

        int sqlDataType = Types.TIMESTAMP;
        minVal = df.parse("2019-04-22 00:00:00").getTime();
        maxVal = df.parse("2019-04-22 23:59:59").getTime();

        String lowClausePrefix = colName + " >= ";
        String highClausePrefix = colName + " < ";

        int numSplits = 1440;
        if (numSplits < 1) {
            numSplits = 1;
        }

        if (minVal == Long.MIN_VALUE && maxVal == Long.MIN_VALUE) {
            // The range of acceptable dates is NULL to NULL. Just create a single
            // split.
            List<InputSplit> splits = new ArrayList<InputSplit>();
            splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                    colName + " IS NULL", colName + " IS NULL"));
            return;
        }

        // For split size we are using seconds. So we need to convert to milliseconds.
        long splitLimit = -1 * MS_IN_SEC;

        // Gather the split point integers
        List<Long> splitPoints = dateSplitter.split(numSplits, splitLimit, minVal, maxVal);
        List<InputSplit> splits = new ArrayList<InputSplit>();

        // Turn the split points into a set of intervals.
        long start = splitPoints.get(0);
        Date startDate = longToDate(start, sqlDataType);
        if (sqlDataType == Types.TIMESTAMP) {
            // The lower bound's nanos value needs to match the actual lower-bound
            // nanos.
            try {
                ((java.sql.Timestamp) startDate).setNanos(0);
            } catch (NullPointerException npe) {
                // If the lower bound was NULL, we'll get an NPE; just ignore it and
                // don't set nanos.
            }
        }

        for (int i = 1; i < splitPoints.size(); i++) {
            long end = splitPoints.get(i);
            Date endDate = longToDate(end, sqlDataType);

            if (i == splitPoints.size() - 1) {
                if (sqlDataType == Types.TIMESTAMP) {
                    // The upper bound's nanos value needs to match the actual
                    // upper-bound nanos.
                    try {
                        ((java.sql.Timestamp) endDate).setNanos(0);
                    } catch (NullPointerException npe) {
                        // If the upper bound was NULL, we'll get an NPE; just ignore it
                        // and don't set nanos.
                    }
                }
                // This is the last one; use a closed interval.
                splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                        lowClausePrefix + dateSplitter.dateToString(startDate),
                        colName + " <= " + dateSplitter.dateToString(endDate)));
            } else {
                // Normal open-interval case.
                splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                        lowClausePrefix + dateSplitter.dateToString(startDate),
                        highClausePrefix + dateSplitter.dateToString(endDate)));
            }

            start = end;
            startDate = endDate;
        }

        if (minVal == Long.MIN_VALUE || maxVal == Long.MIN_VALUE) {
            // Add an extra split to handle the null case that we saw.
            splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                    colName + " IS NULL", colName + " IS NULL"));
        }

        printList(splits);
    }

    private <E> void printList(List<E> list) {
        for (E e : list) {
            System.out.println(e.toString());
        }
    }

    private Date longToDate(long val, int sqlDataType) {
        switch (sqlDataType) {
            case Types.DATE:
                return new java.sql.Date(val);
            case Types.TIME:
                return new java.sql.Time(val);
            case Types.TIMESTAMP:
                return new java.sql.Timestamp(val);
            default: // Shouldn't ever hit this case.
                return null;
        }
    }

}