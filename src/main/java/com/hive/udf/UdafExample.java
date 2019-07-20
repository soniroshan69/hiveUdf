package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;
public class UdafExample extends UDAF
{
	public static class MaxIntUDAFEvaluator implements UDAFEvaluator
	{
		private IntWritable output;
		public void init()
		{
			output=null;
		}
		public boolean iterate(IntWritable maxvalue) // Process input table
		{
			if(maxvalue==null)
			{
				return true;
			}
			if(output == null)
			{
				output = new IntWritable(maxvalue.get());
			}
			else
			{
				output.set(Math.max(output.get(), maxvalue.get()));
			}
			return true;
		}
		public IntWritable terminatePartial()
		{
			return output;
		}
		public boolean merge(IntWritable other)
		{
			return iterate(other);
		}
		public IntWritable terminate() //final result
		{
			return output;
		}
	}
}
