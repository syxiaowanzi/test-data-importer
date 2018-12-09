package org.apache.solr.handle.dataimport;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
//import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
//import org.apache.solr.handler.dataimport.DataImporter;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.apache.solr.handler.dataimport.TemplateTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 实体类处理器
 * 重写EntityProcessorBase
 * @author wangming
 *
 */
public class HbaseEntityProcessor extends EntityProcessorBase{
	  private static final Logger LOG = LoggerFactory.getLogger(TemplateTransformer.class);	
	  protected HbaseDataSource dataSource;
	  private int importIdentifier = 0; //导入标识符
	  private String incrementiImportDate;//按照时间增量参数
	  private String startRow;//起始row
	  private String stopRow; //终止row
	  private long startTime;
	  private long endTime;
	  public static final String QUERY = "query";
	  public static final String DELTA_QUERY = "deltaQuery";
	  public static final String DELTA_IMPORT_QUERY = "deltaImportQuery";
	  public static final String DEL_PK_QUERY = "deletedPkQuery";
	  
	  /**
	   * 初始化实体类调用HbaseDataSouce内部方法
	   */
	  public void init(Context context){
		  super.init(context);
		  //初始化参数
		  String idt = context.getEntityAttribute("importIdentifier");
		  if(!"".equals(idt) && idt!= null) {
			  this.importIdentifier = Integer.parseInt(idt);
		  }
		  switch(importIdentifier) {
		  	case 0://全量导入
		  		break;
		  	case 1://根据日期进行增量导入，日期如20181106
		  		this.incrementiImportDate = context.getEntityAttribute("incrementiImportDate");
		  		if (this.incrementiImportDate == null || "".equals(incrementiImportDate)) {
		  			throw new DataImportHandlerException(500, "incrementiImportDate must be supplied");
		  		}
		  		break;
		  	case 2://根据rowKey进行增量导入，startRow&stopRow导入不包含stopRow
		  		this.startRow = context.getEntityAttribute("startRow");
		  		if (this.startRow == null || "".equals(startRow)) {
		  			throw new DataImportHandlerException(500, "startRow must be supplied");
		  		}
		  		this.stopRow = context.getEntityAttribute("stopRow");
		  		break;
		  	case 3://根据开始时间和结束时间进行增量数据
		  		try {
		  			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		  			Date date = null;
		  			String start = context.getEntityAttribute("startTime");
		  			if (start== null || "".equals(start)) {
		  				throw new DataImportHandlerException(500, "startTime must be supplied");
		  			}else {
		  				date = simpleDateFormat.parse(start);
		  				this.startTime = date.getTime();
		  			}
		  			String end = context.getEntityAttribute("endTime");
		  			if (end == null || "".equals(end)) {
		  				throw new DataImportHandlerException(500, "endTime must be supplied");
		  			}else {
		  				date = simpleDateFormat.parse(start);
		  				this.endTime = date.getTime();
		  			}
		  		} catch (ParseException e) {
					e.printStackTrace();
				}
		  		break;	
		  	default :
		  		break;
		  }
		  this.dataSource = ((HbaseDataSource)context.getDataSource());
	  }
	  
	  /**
	   * 全量导入方法
	   * 初始化查询条件调用查询方法
	   * @param 查询条件
	   */
	  protected void initQuery(String q){
		  try{
			  LOG.info("******************initQuery()*******************");
//			  ((AtomicLong)DataImporter.QUERY_COUNT.get()).incrementAndGet();
			  switch(importIdentifier) {
			  	case 0://全量导入
			  		this.rowIterator = this.dataSource.getData(q);
			  		break;
			  	case 1:
			  		this.rowIterator = this.dataSource.getDataByRowDate(incrementiImportDate);
			  		break;
			  	case 2:
			  		this.rowIterator = this.dataSource.getDataByRowKey(startRow,stopRow);
			  		break;
			  	case 3:
			  		this.rowIterator = this.dataSource.getDataByTime(startTime,endTime);
			  		break;	
			  	default :
			  		this.rowIterator = this.dataSource.getData(q);
			  		break;
			  }		
			  this.query = q;
		  }catch (DataImportHandlerException e){
			  throw e;
		  }catch (Exception e){
			  throw new DataImportHandlerException(500, e);
		  }
	  }
	  /**
	   * 增量导入方法
	   * 初始化查询条件调用查询方法
	   * @param q 查询条件
	   * @param dq 执行增量查询-增量导入
	   */
	  protected void initQueryForDelta(String q ,boolean dq){
		  try{
			  LOG.info("******************initQueryForDelta*******************");	
	//	      ((AtomicLong)DataImporter.QUERY_COUNT.get()).incrementAndGet();
			  if(dq) {
				  this.rowIterator = this.dataSource.getDataDeltaQuery(q);
			  }else {
				  this.rowIterator = this.dataSource.getDataDeltaImportQuery(q);
			  }
//			  this.rowIterator = this.dataSource.getData(q,dq);
			  this.query = q;
		  }catch (DataImportHandlerException e){
			  throw e;
		  }catch (Exception e){
			  throw new DataImportHandlerException(500, e);
		  }
	  }
	  
	  /**
	   * 判断全量导入还是增量导入执行不同方法
	   */
	  public Map<String, Object> nextRow(){
		  LOG.info("******************nextRow()*****************");
		  if (this.rowIterator == null){
			  String query = getQuery();
			  if("FULL_DUMP".equals(this.context.currentProcess())) {
				  LOG.info("******************FULL_DUMP nextRow*****************");
				  initQuery(this.context.replaceTokens(query));
			  }else {
				  LOG.info("******************deltaImportQuery nextRow*****************");
				  initQueryForDelta(this.context.replaceTokens(query),false);
			  }
		  }
		  return getNext();
	  }
	  /**
	   * 增量导入之前的查询
	   */
	  public Map<String, Object> nextModifiedRowKey(){
		  LOG.info("******************nextModifiedRowKey*****************");
		  if (this.rowIterator == null){
			  String deltaQuery = this.context.getEntityAttribute("deltaQuery");
			  if (deltaQuery == null) {
				  return null;
			  }
			  initQueryForDelta(this.context.replaceTokens(deltaQuery),true);
		  }
		  return getNext();
	  }
	  
	  public Map<String, Object> nextDeletedRowKey(){
		  LOG.info("******************nextDeletedRowKey*****************");
		  if (this.rowIterator == null){
			  String deletedPkQuery = this.context.getEntityAttribute("deletedPkQuery");
			  if (deletedPkQuery == null) {
				  return null;
			  }
			  initQuery(this.context.replaceTokens(deletedPkQuery));
		  }
		  return getNext();
	  }
	  
	  public Map<String, Object> nextModifiedParentRowKey(){
		  LOG.info("******************nextModifiedParentRowKey*****************");
		  if (this.rowIterator == null){
			  String parentDeltaQuery = this.context.getEntityAttribute("parentDeltaQuery");
			  if (parentDeltaQuery == null) {
				  return null;
			  }
			  initQuery(this.context.replaceTokens(parentDeltaQuery));
		  }
		  return getNext();
	  }
	  /**
	   * 根据全量导入还是增量导入返回不同参数
	   * @return 查询条件
	   */
	  public String getQuery(){
		  String queryString = this.context.getEntityAttribute("query");
		  if ("FULL_DUMP".equals(this.context.currentProcess())) {
			  LOG.info("**************执行全量导入********************");
			  return queryString;
		  }
		  if ("DELTA_DUMP".equals(this.context.currentProcess())) {
			  LOG.info("**************执行增量导入********************");
			  return this.context.getEntityAttribute("deltaImportQuery");
		  }
		  return null;
	  }
	  
}
