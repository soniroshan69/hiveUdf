package com.hive.udf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

class ComplexUDF extends GenericUDF
{
	ListObjectInspector listOI;
	IntObjectInspector elementOI;
	@Override
	public String getDisplayString(String[] arg0)
	{
		return "arrayContainsExample()"; // this should probably be better
	}
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException
	{
		if (arguments.length != 2)
		{
			throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: List<T>, T");
		}
		// 1. Check we received the right object types.
		ObjectInspector a = arguments[0];
		ObjectInspector b = arguments[1];
		if (!(a instanceof ListObjectInspector) || !(b instanceof IntObjectInspector))
		{
			throw new UDFArgumentException("first argument must be a list / array, second argument must be a int");
		}
		this.listOI = (ListObjectInspector) a;
		this.elementOI = (IntObjectInspector) b;
		// 2. Check that the list contains strings
		if(!(listOI.getListElementObjectInspector() instanceof IntObjectInspector))
		{
			throw new UDFArgumentException("first argument must be a list of int");
		} 
		// the return type of our function is a boolean, so we provide the correct object inspector
		return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
	}
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException
	{  
		// get the list and string from the deferred objects using the object inspectors
		List<Integer> list = (List<Integer>) this.listOI.getList(arguments[0].get());
		Integer arg = Integer.valueOf(elementOI.getPrimitiveJavaObject(arguments[1].get()).toString());  
		// check for nulls
		if (list == null || arg == null)
		{
			return null;
		} 
		// see if our list contains the value we need
		for(Integer s: list)
		{
			if (arg.equals(s)) return new Boolean(true);
		}
		return new Boolean(false);
	}
}
