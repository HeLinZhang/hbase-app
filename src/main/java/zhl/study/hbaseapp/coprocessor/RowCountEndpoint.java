/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年4月18日
 * @Copyright ZHL All Rights Reserved.
 */
package zhl.study.hbaseapp.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.slf4j.LoggerFactory;

import zhl.study.hbaseapp.coprocessor.proto.RowCount;
import zhl.study.hbaseapp.coprocessor.proto.RowCount.RowCountRequest;
import zhl.study.hbaseapp.coprocessor.proto.RowCount.RowCountResponse;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class RowCountEndpoint extends RowCount.RowCountService implements
		Coprocessor, CoprocessorService {

	private static final org.slf4j.Logger logger = LoggerFactory
			.getLogger(RowCountEndpoint.class);

	private RegionCoprocessorEnvironment env;

	public RowCountEndpoint() {
	}

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void start(CoprocessorEnvironment envi) throws IOException {
		 if (envi instanceof RegionCoprocessorEnvironment) {
		      this.env = (RegionCoprocessorEnvironment)envi;
		      logger.info("RowCountEndpoint-start被调用了");
		    } else {
		    logger.error("RowCountEndpoint-msg={Must be loaded on a table region!}");
		    throw new CoprocessorException("Must be loaded on a table region!");
		    }
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
	    // nothing to do
	    logger.info("RowCountEndpoint-stop被调用了");
	}

	@Override
	public void getRowCount(RpcController controller, RowCountRequest request,
			RpcCallback<RowCountResponse> done) {
		logger.info("request.allFields={}",request.getAllFields());
		logger.error("RowCountEndpoint-getRowCount-scanner正在准备中.....");
	    long rowcount = 0;
		InternalScanner scanner = null;
	      try{
	        Scan scan =new Scan();
	        scanner = env.getRegion().getScanner(scan);
	        List<Cell> results = new ArrayList<Cell>();
	        boolean hasMore = false;

	        do {
	          hasMore = scanner.next(results);
	          rowcount++;
	        }while (hasMore);
	      }
	      catch (IOException ioe) {
	    	  logger.error("RowCountEndpoint-getRowCount-IOException={}",ioe);
	      }
	      finally {
	        if (scanner != null) {
	          try {
	            scanner.close();
	          } catch (IOException ignored) {}
	        }
	        logger.error("RowCountEndpoint-getRowCount-sannner结束了.....rowcount={}",rowcount);
	      }
		
	      RowCount.RowCountResponse.Builder responseBuilder = RowCount.RowCountResponse.newBuilder(); 
	      RowCount.RowCountResult.Builder reCount= RowCount.RowCountResult.newBuilder();
	      reCount.setKey(env.getRegion().getRegionNameAsString());
	      reCount.addVal(String.valueOf(rowcount));
	      responseBuilder.addResult(reCount);
	      done.run(responseBuilder.build());
	}

}
