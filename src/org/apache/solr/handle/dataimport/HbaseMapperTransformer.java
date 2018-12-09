package org.apache.solr.handle.dataimport;

import java.util.Map;

import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.Transformer;

/**
 * @author wangming
 * 它的配置是灵活的。它允许用户向标签entity和field提供任意的属性。
 * tool将会读取数据，并将它传给实现类。如果 Transformer需要额外
 * 的的信息，它可以从context中取得。
 */
public class HbaseMapperTransformer extends Transformer{
	
	public static final String HBASE_FIELD = "hbaseField";
  
	public Object transformRow(Map<String, Object> row, Context context){
		for (Map<String, String> map : context.getAllEntityFields()){
			String hbaseFieldName = (String)map.get("hbaseField");
			if (hbaseFieldName != null){
				String columnFieldName = (String)map.get("column");
				row.put(columnFieldName, row.get(hbaseFieldName));
			}
		}
		return row;
	}
}
