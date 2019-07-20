package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class SimpleUDF extends UDF { 

	public String evaluate(String input)
    {
          return input.toUpperCase(); 
    }
}
