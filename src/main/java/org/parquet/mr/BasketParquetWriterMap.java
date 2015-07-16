package org.parquet.mr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import parquet.example.data.Group;
import parquet.schema.PrimitiveType.PrimitiveTypeName;



/*
 * 
 * 最后输出的数据为<productid,count>
 */
public class BasketParquetWriterMap extends
		Mapper<Text, Group, Text, IntWritable> {


	@Override
	public void map(Text key, Group value, Context context) throws IOException,
			InterruptedException {
	

		String productid = getGaroupValue(value, "productid");
		if (StringUtils.isEmpty(productid)) {
			return;
		}
		context.write(new Text(productid), new IntWritable(1));
	}

	/**
	 * 
	 * @param group
	 * @param fieldName
	 * @return
	 */
	private String getGaroupValue(Group group, String fieldName) {
		if (0 == group.getFieldRepetitionCount(fieldName))
			return null;
		String ret = null;
		PrimitiveTypeName typeName = group.getType().getType(fieldName)
				.asPrimitiveType().getPrimitiveTypeName();
		switch (typeName) {
		case BINARY:
		case FIXED_LEN_BYTE_ARRAY:
		case INT96:
			ret = group.getBinary(fieldName, 0).toStringUsingUTF8();
			break;
		case INT64:
			ret = String.valueOf(group.getLong(fieldName, 0));
			break;
		case INT32:
			ret = String.valueOf(group.getInteger(fieldName, 0));
			break;
		case BOOLEAN:
			ret = String.valueOf(group.getBoolean(fieldName, 0));
			break;
		default:
			throw new UnsupportedOperationException(group.getType()
					.asPrimitiveType().getName()
					+ " not supported for Binary");
		}
		return ret;
	}

	
}
