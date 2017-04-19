/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年4月19日
 * @Copyright ZHL All Rights Reserved.
 */
package zhl.study.hbaseapp.coprocessor;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.LoggerFactory;

public class RowCountObserverServer {

	private static final org.slf4j.Logger logger = LoggerFactory
			.getLogger(RowCountEndPointServer.class);

	public static void main(String[] args) throws IOException {
		String tblName="fz_tag_15";
		int num =1000;
		boolean delFlag=false;
		if (args.length ==3) {
			tblName=args[0];
			if ("del".equals(args[1])) {
				delFlag=true;
			}
			num=Integer.valueOf(args[2]);
		}else{
			System.out.println("args: tableName add|del num");
			System.exit(0);
		}
		
		RowCountObserverServer rcObs = new RowCountObserverServer();
		rcObs.add(tblName, num);
		logger.info("准备发出add请求,num={}",num);
		if(delFlag){
			rcObs.del(tblName, num);
			logger.info("准备发出del请求,num={}",num);
		}
		
		logger.info("RowCountObserverServer--程序完成并exit");
	}
	

	void add(String tableName, int num) throws IOException {
		Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum", "10.111.32.203");
		HConnection conn = HConnectionManager.createConnection(config);
		HTableInterface tbl = conn.getTable(tableName);

		// insert 1000
		for (int i = 0; i < num; i++) {
			String rowkey = "000056a_"
					+ DigestUtils.md5Hex(Integer.toString(i));
			Put put = new Put(rowkey.getBytes());
			put.add("d".getBytes(), "mdn".getBytes(), ("v" + i).getBytes());
			logger.info("add-准备发送第{}条数据rowkey={}",i,rowkey);
			tbl.put(put);
			Get get= new Get(rowkey.getBytes());
			Result getVal=tbl.get(get);
			logger.info("add-发送成功{}条数据,查询rowkey={},",i,rowkey,getVal.getRow());
		}
		
	}
	
	

	void del(String tableName, int num) throws IOException {

		Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum", "10.111.32.203");
		HConnection conn = HConnectionManager.createConnection(config);
		HTableInterface tbl = conn.getTable(tableName);

		for (int i = 0; i < num ; i++) {
			String rowkey = "000056a_"
					+ DigestUtils.md5Hex(Integer.toString(i));
			logger.info("del-准备发送第{}条数据rowkey={}",i,rowkey);
			Delete d = new Delete(rowkey.getBytes());
			tbl.delete(d);
		}
	}

}
