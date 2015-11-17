package org.apache.sqoop.submission.spark;
import java.util.Iterator;
import java.util.List;

import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.etl.io.DataReader;

public class SparkDataReader extends DataReader {

    private Iterator<IntermediateDataFormat<?>> dataIterator = null;

    public SparkDataReader(List<IntermediateDataFormat<?>> data) {
        this.dataIterator = (data).iterator();
    }

    @Override
    public Object[] readArrayRecord() throws InterruptedException {
        if (dataIterator.hasNext()) {
            IntermediateDataFormat<?> element = dataIterator.next();
            return element.getObjectData();
        }
        return null;
    }

    @Override
    public String readTextRecord() throws InterruptedException {
        if (dataIterator.hasNext()) {
            IntermediateDataFormat<?> element = dataIterator.next();
            return element.getCSVTextData();
        }
        return null;
    }

    @Override
    public Object readContent() throws InterruptedException {
        if (dataIterator.hasNext()) {
            IntermediateDataFormat<?> element = dataIterator.next();
            return element.getData();
        }
        return null;
    }

}
