package org.apache.sqoop.job;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class TestSparkPrefixContext {

    private static final String PREFIX= "p.";
    private static final String TEST_KEY= "testkey";
    private static final String TEST_VALUE= "testvalue";
    private static final String DEFAULT_VALUE= "defaultValue";

    @Test
    public void testBlankPrefix() {
        Map<String, String> options= new HashMap<>();
        options.put(TEST_KEY, TEST_VALUE);

        SparkPrefixContext context = new SparkPrefixContext(options, "");
        assertEquals(TEST_VALUE, context.getString(TEST_KEY));
    }

    @Test
    public void testGetOptions() throws Exception {
        Map<String, String> options= new HashMap<>();
        options.put(PREFIX + TEST_KEY, TEST_VALUE);

        SparkPrefixContext context= new SparkPrefixContext(options, PREFIX);
        assertEquals(1, context.getOptions().size());
    }

    @Test
    public void testGetString() throws Exception {
        Map<String, String> options= new HashMap<>();
        options.put(PREFIX + TEST_KEY, TEST_VALUE);

        SparkPrefixContext context= new SparkPrefixContext(options, PREFIX);
        assertEquals(TEST_VALUE, context.getString(TEST_KEY));
        assertEquals(TEST_VALUE, context.getString(TEST_KEY, DEFAULT_VALUE));
        assertEquals(DEFAULT_VALUE, context.getString("wrongKey", DEFAULT_VALUE));
    }


    @Test
    public void testGetBoolean() throws Exception {
        Map<String, String> options= new HashMap<>();
        options.put(PREFIX + TEST_KEY, "true");

        SparkPrefixContext context= new SparkPrefixContext(options, PREFIX);
        assertEquals(true, context.getBoolean(TEST_KEY, false));
        assertEquals(false, context.getBoolean("wrongKey", false));
    }

    @Test
    public void testGetLong() throws Exception {
        Map<String, String> options= new HashMap<>();
        options.put(PREFIX + TEST_KEY, "123");

        SparkPrefixContext context= new SparkPrefixContext(options, PREFIX);
        assertEquals(123L, context.getLong(TEST_KEY, 456L));
        assertEquals(456L, context.getLong("wrongKey", 456L));
    }

    @Test
    public void testGetInt() throws Exception {
        Map<String, String> options= new HashMap<>();
        options.put(PREFIX + TEST_KEY, "123");

        SparkPrefixContext context= new SparkPrefixContext(options, PREFIX);
        assertEquals(123, context.getInt(TEST_KEY, 456));
        assertEquals(456, context.getInt("wrongKey", 456));
    }

    @Test
    public void testIterator() throws Exception {
        Map<String, String> options= new HashMap<>();
        options.put(PREFIX + "element1", "value 1");
        options.put(PREFIX + "element2", "value 2");

        SparkPrefixContext context= new SparkPrefixContext(options, PREFIX);
        boolean seenElement1= false;
        boolean seenElement2= false;

        for(Map.Entry<String, String> entry : context) {
            if("element1".equals(entry.getKey()) && "value 1".equals(entry.getValue())) {
                seenElement1 = true;
            } else if("element2".equals(entry.getKey()) && "value 2".equals(entry.getValue())) {
                seenElement2 = true;
            } else {
                fail("Found unexpected property: " + entry.getKey() + " with value " + entry.getValue());
            }
        }

        assertTrue(seenElement1);
        assertTrue(seenElement2);

    }
}