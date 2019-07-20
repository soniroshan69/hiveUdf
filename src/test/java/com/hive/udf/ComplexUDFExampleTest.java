package com.hive.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class ComplexUDFExampleTest
{
@Test
public void testComplexUDFReturnsCorrectValues() throws HiveException
   {
//set up the models we need
      ComplexUDF example = new ComplexUDF();
      ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
      ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
      JavaBooleanObjectInspector resultInspector = (JavaBooleanObjectInspector) example.initialize(new ObjectInspector[]{listOI, intOI}); 
//create the actual UDF arguments
      List<Integer> list = new ArrayList<Integer>();
      list.add(1);
      list.add(2);
      list.add(3);
      Integer[] array = list.toArray(new Integer[list.size()]);
//test our results 
//the value exists
      Object result = example.evaluate(new DeferredObject[]{new DeferredJavaObject(array), new DeferredJavaObject(1)});
      Assert.assertEquals(true, resultInspector.get(result)); 
//the value doesn't exist
      Object result2 = example.evaluate(new DeferredObject[]{new DeferredJavaObject(array), new DeferredJavaObject(4)});
      Assert.assertEquals(false, resultInspector.get(result2));  
//arguments are null
      //Object result3 = example.evaluate(new DeferredObject[]{new DeferredJavaObject(null), new DeferredJavaObject(null)});
      //Assert.assertNull(result3);
      
      try {
		example.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
}
