package org.apache.sqoop.util;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSqoopJsonUtil {

  private SqoopJsonUtil jsonUtil;
  private static Map<String, String> paramMap;
  private static String jsonStr;

  @BeforeClass
  public static void setup() {
    paramMap = new HashMap<String, String>();
    paramMap.put("k1", "v1");
    paramMap.put("k2", "v2");
    paramMap.put("k3", "v3");

    jsonStr = "{\"k3\":\"v3\",\"k1\":\"v1\",\"k2\":\"v2\"}";

  }

  @Test
  public void testGetJsonStringFromMap() {
    String resultJsonStr = SqoopJsonUtil.getJsonStringforMap(paramMap);
    assertEquals(jsonStr, resultJsonStr);
  }

  @Test
  public void testGetJsonStringFromMapNullMap() {
    Map<String, String> nullMap = null;
    String resultJsonStr = SqoopJsonUtil.getJsonStringforMap(nullMap);
    assertEquals("{}", resultJsonStr);
  }

  @Test
  public void testGetJsonStringFromMapEmptyMap() {
    Map<String, String> nullMap = new HashMap<String, String>();
    String resultJsonStr = SqoopJsonUtil.getJsonStringforMap(nullMap);
    assertEquals("{}", resultJsonStr);
  }

  @Test
  public void testGetMapforJsonString() {
    Map<String, String> resultMap = SqoopJsonUtil.getMapforJsonString(jsonStr);
    assertEquals(paramMap, resultMap);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMapforJsonStringNullString() {
    Map<String, String> resultMap = SqoopJsonUtil.getMapforJsonString(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMapforJsonStringEmptyString() {
    Map<String, String> resultMap = SqoopJsonUtil.getMapforJsonString("");
  }

  @Test
  public void testEmptyJSON() {
    String jsonStr = null;
    boolean isEmpty;
    isEmpty = SqoopJsonUtil.isEmptyJSON(jsonStr);
    assertEquals(true, isEmpty);

    jsonStr = "";
    isEmpty = SqoopJsonUtil.isEmptyJSON(jsonStr);
    assertEquals(true, isEmpty);

    jsonStr = "{}";
    isEmpty = SqoopJsonUtil.isEmptyJSON(jsonStr);
    assertEquals(true, isEmpty);

  }

  @Test
  public void testNonEmptyJSON() {
    boolean isEmpty = SqoopJsonUtil.isEmptyJSON(jsonStr);
    assertEquals(false, isEmpty);
  }

}
