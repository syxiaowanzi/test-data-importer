package org.apache.solr.handle.dataimport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.DataSource;
import org.apache.solr.handler.dataimport.TemplateTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author wangming
 * 继承solr DataSource类，重写方法
 * 通过init与habase建立连接
 * 通过getData方法返回solr所需要的迭代数据
 */
public class HbaseDataSource extends DataSource{
    
	private static final Logger LOG = LoggerFactory.getLogger(TemplateTransformer.class);
	private Configuration configuration;	    // 定义配置对象
    private Connection Hbaseconn;			    // 定义连接
    private ResultScanner resultScanner = null;	//habse返回结果
    String tableName = "";
    Iterator<Result>  iteratorResult;			//结果迭代
    /**
            * 根据data-config.xml配置的参数
            * 建立hbase连接
     */
    public void init(Context context, Properties initProps){
    	LOG.info("********init datasouce get database connection********");
    	String host = initProps.getProperty("host", "localhost");
        String port = initProps.getProperty("port", "2181");
        tableName = initProps.getProperty("tableName");
        try {
        	configuration = HBaseConfiguration.create();
        	configuration.set("hbase.zookeeper.quorum",host);  
            configuration.set("hbase.zookeeper.property.clientPort",port); 
            Connection conn = ConnectionFactory.createConnection(configuration);
            this.Hbaseconn = conn;
        }catch (Exception e){
        	throw new DataImportHandlerException(500, "Unable to connect to hbase");
        }
    }
	/**
	 * 全量导入处理
	 * @param query 查询条件 query = FULL_DUMP 全量导入
	 * @return 返回Iterator<Map<String, Object>>类型的迭代器
	 * 供solr接收处理
	 */
    public Iterator<Map<String, Object>> getData(String query){
    	LOG.info("********HbaseDataSource getData running********");
    	HTable table = null;
        try {
			table = (HTable) this.Hbaseconn.getTable(TableName.valueOf(this.tableName));
			Scan scan = new Scan();
            this.resultScanner = table.getScanner(scan);
        } catch (IOException e) { 
            e.printStackTrace(); 
        }
        if(resultScanner != null ) {
        	this.iteratorResult =  this.resultScanner.iterator();
        }
        ResultSetIterator resultSet = new ResultSetIterator(this.iteratorResult,true);
      	return resultSet.getIterator();
    }
    
    /**
            * 根据rowKey中包含的日期进行过滤获得所有包含日期的数据
     * @param importByDate  时间 例如20181107
     */
    public Iterator<Map<String, Object>> getDataByRowDate(String importByDate){
    	LOG.info("********HbaseDataSource getDataByRowDate running********");
    	HTable table = null;
        try {
			table = (HTable) this.Hbaseconn.getTable(TableName.valueOf(this.tableName));
			RowFilter rf = new RowFilter(CompareOp.EQUAL ,new SubstringComparator(importByDate));
	        Scan scan = new Scan();
	        scan.setFilter(rf);
            this.resultScanner = table.getScanner(scan);
        } catch (IOException e) { 
            e.printStackTrace(); 
        }
        if(resultScanner != null ) {
        	this.iteratorResult =  this.resultScanner.iterator();
        }
        ResultSetIterator resultSet = new ResultSetIterator(this.iteratorResult,true);
      	return resultSet.getIterator();
    }
    
    /**
            * 根据rowKey可以获取数据
     * @param startRow 开始row
     * @param stopRow  结束row
     */
    public Iterator<Map<String, Object>> getDataByRowKey(String startRow, String stopRow){
    	LOG.info("********HbaseDataSource getDataByRowDate running********");
    	HTable table = null;
        try {
			table = (HTable) this.Hbaseconn.getTable(TableName.valueOf(this.tableName));
	        Scan scan = new Scan();
	        scan.setStartRow(Bytes.toBytes(startRow));
	        if(stopRow != null && stopRow != null) {
	        	scan.setStopRow(Bytes.toBytes(stopRow));
	        }
            this.resultScanner = table.getScanner(scan);
        } catch (IOException e) { 
            e.printStackTrace(); 
        }
        if(resultScanner != null ) {
        	this.iteratorResult =  this.resultScanner.iterator();
        }
        ResultSetIterator resultSet = new ResultSetIterator(this.iteratorResult,true);
      	return resultSet.getIterator();
    }
    
    /**
             * 根据开始时间结束时间增量数据
     * @param startTime 开始时间
     * @param sendTime 结束时间
     */
    public Iterator<Map<String, Object>> getDataByTime(long startTime, long sendTime){
    	LOG.info("********HbaseDataSource getDataByTime running********");
    	HTable table = null;
        try {
			table = (HTable) this.Hbaseconn.getTable(TableName.valueOf(this.tableName));
	        Scan scan = new Scan();
	        scan.setTimeRange(startTime, sendTime);
            this.resultScanner = table.getScanner(scan);
        } catch (IOException e) { 
            e.printStackTrace(); 
        }
        if(resultScanner != null ) {
        	this.iteratorResult =  this.resultScanner.iterator();
        }
        ResultSetIterator resultSet = new ResultSetIterator(this.iteratorResult,true);
      	return resultSet.getIterator();
    }
    
    
    /**
             * 目前废弃的方法
             * 增量导入
             * 增量导入分为两个阶段
             * 第一阶段根据上次导入的时间查询出新添加修改的数据返回迭代的id值
             * 第二阶段为根据迭代的id查询出所需要导入的数据
     * query = 时间  & deltaQuery = false ，为增量导入查询阶段（deltaQuery） 
     * query = id的迭代 & deltaQuery = true 为执行增量导入（deltaImportQuery）
     * @return 迭代的结果集
     */
    public Iterator<Map<String, Object>> getData(String query, boolean deltaQuery){
    	LOG.info("********HbaseDataSource deltaQuery getData running********");
    	HTable table = null;
    	boolean sign = true;
        try {
			table = (HTable) this.Hbaseconn.getTable(TableName.valueOf(this.tableName));
			Scan scan = new Scan();
			if(deltaQuery) {
				//第一阶段根据时间查询所需要导入的数据
				try {
					LOG.info("************deltaQuery 时间："+query+"************");
					SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					Date date = simpleDateFormat.parse(query);
					long stime = date.getTime();
					long etime = System.currentTimeMillis();
					scan.setTimeRange(stime, etime);
					sign = false;
				}catch(Exception e){
					throw new DataImportHandlerException(500, e);
				}
			}else {
				
				LOG.info("************deltaImportQuery id 值："+query+"************");
				//第二阶段根据rowkey查询数据返回迭代结果
		        scan.setStartRow(query.getBytes());
		        scan.setStopRow(query.getBytes());
		        sign = true;
			}
			this.resultScanner = table.getScanner(scan);
        } catch (IOException e) { 
            e.printStackTrace(); 
        }
        if(resultScanner !=null ) {
        	this.iteratorResult =  this.resultScanner.iterator();
        }
        ResultSetIterator resultSet = new ResultSetIterator(this.iteratorResult,sign);
        
      	return resultSet.getIterator();
    }
    
    /**
             * 增量导入查询的方法所有需要增量的rowKey，迭代返回
     * @param query 上次solr导入时间时间
     * @return 所有需要导入的RowKey
     */
    public Iterator<Map<String, Object>> getDataDeltaQuery(String query){
    	LOG.info("********HbaseDataSource deltaQuery getData running********");
    	LOG.info("********query********"+query);
    	HTable table = null;
        try {
			table = (HTable) this.Hbaseconn.getTable(TableName.valueOf(this.tableName));
			Scan scan = new Scan();
			//第一阶段根据时间查询所需要导入的数据
			try {
				LOG.info("************deltaQuery 时间："+query+"************");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date date = simpleDateFormat.parse(query);
				long stime = date.getTime();
				long etime = System.currentTimeMillis();
				scan.setTimeRange(stime, etime);
			}catch(Exception e){
				throw new DataImportHandlerException(500, e);
			}
			
			this.resultScanner = table.getScanner(scan);
        } catch (IOException e) { 
            e.printStackTrace(); 
        }
        if(resultScanner !=null ) {
        	this.iteratorResult =  this.resultScanner.iterator();
        }
        ResultSetIterator resultSet = new ResultSetIterator(this.iteratorResult,false);
        
      	return resultSet.getIterator();
    }
    /***
             * 根据rowkey获取需要数据组装迭代进行增量导入
     * @param query RowKey
     * @return 
     */
    public Iterator<Map<String, Object>> getDataDeltaImportQuery(String query){
    	LOG.info("********HbaseDataSource deltaImportQuery getData running********");
    	HTable table = null;
    	Iterator<Map<String, Object>> rSetIterator = null;
    	Map<String, Object> map = new HashMap<String, Object>();
        try {
			table = (HTable) this.Hbaseconn.getTable(TableName.valueOf(this.tableName));
			Get get = new Get(query.getBytes());
			Result res = table.get(get);
			String keyId = Bytes.toString(res.getRow());
			map.put("id", keyId);
			for (KeyValue keyValue : res.raw()) {
				map.put(Bytes.toString(keyValue.getQualifier()), Bytes.toString(keyValue.getValue()));
			}
			rSetIterator = new Iterator<Map<String, Object>>(){
				boolean sign = true;
    			public boolean hasNext(){
    				return sign;
    			}
          
    			public Map<String, Object> next(){
    				sign = false;
    				return map;
    			}
          
    			public void remove() {}
    		}; 
        } catch (IOException e) { 
            e.printStackTrace(); 
        }
      	return rSetIterator;
    }
    
    /***
 	  * 在ResultSetIterator内部类，根据封装
     * resultScanner数据集，返回
     * Iterator<Map<String, Object>>类型数据迭代器
     */
    private class ResultSetIterator{
    	Iterator<Result> iteratorResult;
    	Iterator<Map<String, Object>> rSetIterator;
    	boolean sign = true;
      
    	public ResultSetIterator(Iterator<Result> iteratorResult,boolean sign){
    		this.iteratorResult = iteratorResult;
    		this.sign = sign;

    		this.rSetIterator = new Iterator<Map<String, Object>>(){
    			public boolean hasNext(){
    				return HbaseDataSource.ResultSetIterator.this.hasnext();
    			}
          
    			public Map<String, Object> next(){
    				return HbaseDataSource.ResultSetIterator.this.getARow();
    			}
          
    			public void remove() {}
    		};
    	}
      
    	public Iterator<Map<String, Object>> getIterator(){
    		return this.rSetIterator;
    	}
      
    	private Map<String, Object> getARow(){
    		Map<String, Object> result = new HashMap<String, Object>();
    		try {
    			Result res = HbaseDataSource.this.getIteratorResult().next();
    			String keyId = Bytes.toString(res.getRow());
    			result.put("id", keyId);
    			if(sign) {
    				for (KeyValue keyValue : res.raw()) {
    					result.put(Bytes.toString(keyValue.getQualifier()), Bytes.toString(keyValue.getValue()));
    				}
    			}
    		}catch (Exception e) {
//    			LOG.warn(e.getMessage());
				// TODO: handle exception
    			close();
    			DataImportHandlerException.wrapAndThrow(500, e);
			}
    		return result;
    	}
      
    	private boolean hasnext(){
    		if (this.iteratorResult == null) {
    			return false;
    		}
    		try{
    			if (this.iteratorResult.hasNext()) {
    				return true;
    			}
    			close();
    			return false;
    		}catch (Exception e){
    			close();
    			DataImportHandlerException.wrapAndThrow(500, e);
    		}
    		return false;
    	}
      
    	private void close(){
    		try{
    			if (resultScanner != null) {
    				resultScanner.close();
    			}
    		}catch (Exception e){
    			System.out.println(e.getMessage());
    		}finally{
    			resultScanner = null;
    		}
    	}
    	
	}
    
	private Iterator<Result>  getIteratorResult(){
      return this.iteratorResult;
    }
	
    public void close(){
    	LOG.info("***********colse hbase conn!************");
        if (this.resultScanner != null) {
        	this.resultScanner.close();
        }
        if (this.Hbaseconn != null) {
    	   try {
    		   this.Hbaseconn.close();
    	   } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
    	   }
      }
    }
    
}
