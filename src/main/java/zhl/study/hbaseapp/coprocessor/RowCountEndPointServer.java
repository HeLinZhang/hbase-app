/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年4月18日
 * @Copyright ZHL All Rights Reserved.
 */
package zhl.study.hbaseapp.coprocessor;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;

import zhl.study.hbaseapp.coprocessor.proto.RowCount.RowCountParam;
import zhl.study.hbaseapp.coprocessor.proto.RowCount.RowCountRequest;
import zhl.study.hbaseapp.coprocessor.proto.RowCount.RowCountResponse;
import zhl.study.hbaseapp.coprocessor.proto.RowCount.RowCountResult;
import zhl.study.hbaseapp.coprocessor.proto.RowCount.RowCountService;

public class RowCountEndPointServer {

	private static final org.slf4j.Logger logger = LoggerFactory
			.getLogger(RowCountEndPointServer.class);

	public static void main(String[] args) throws ServiceException, Throwable {
		String tblName = "fz_tag_15";

		boolean fastFlag = false;

		if (args.length ==2) {
			tblName=args[0];
			if ("fast".equals(args[1])) {
				fastFlag = true;
			}
		}else{
			System.out.println("args: tableName fast|slow");
			System.exit(0);
		}
		
		RowCountEndPointServer rcEndPointServer = new RowCountEndPointServer();
		Long wholeCount = 0L;
		if (fastFlag) {
			wholeCount = rcEndPointServer.wholeTableCountFast(tblName);
		} else {
			wholeCount = rcEndPointServer.wholeTableCountSlow(tblName);
		}
		logger.info("wholeCount={}", wholeCount);
		logger.info("RowCountEndPointServer--程序完成并exit");
	}

	/**
	 * @param tblName
	 * @return
	 * @throws Throwable
	 * @throws ServiceException
	 */
	private Long wholeTableCountSlow(final String tblName)
			throws ServiceException, Throwable {
		logger.info("wholeTableCount-tab={}", tblName);
		Map<byte[], RowCountResponse> results = null;

		Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum", "10.111.32.203");
		HConnection connection = HConnectionManager.createConnection(config);
		HTableInterface table = connection.getTable(tblName);

		Batch.Call<RowCountService, RowCountResponse> callable = new Batch.Call<RowCountService, RowCountResponse>() {
			ServerRpcController controller = new ServerRpcController();
			BlockingRpcCallback<RowCountResponse> rpcCallback = new BlockingRpcCallback<RowCountResponse>();

			@Override
			public RowCountResponse call(RowCountService instance)
					throws IOException {
				RowCountRequest.Builder rcReqBuilder = RowCountRequest
						.newBuilder();
				RowCountParam.Builder rcParamBuilder = RowCountParam
						.newBuilder();
				rcParamBuilder.setKey("htab");
				rcParamBuilder.addVal(tblName);
				rcReqBuilder.addContext(rcParamBuilder);

				instance.getRowCount(controller, rcReqBuilder.build(),
						rpcCallback);
				return rpcCallback.get();
			}
		};

		results = table.coprocessorService(RowCountService.class, null, null,
				callable);

		Long totalRowCount = 0L;
		Collection<RowCountResponse> resultsc = results.values();
		for (RowCountResponse res : resultsc) {
			Long regionRowCount = 0L;

			List<RowCountResult> rlist = res.getResultList();
			for (RowCountResult rcres : rlist) {

				List<String> lr = rcres.getValList();
				for (String r : lr) {
					regionRowCount = regionRowCount + Long.valueOf(r);
				}
				logger.info("region-name={},region-rowcount={}",
						rcres.getKey(), regionRowCount);
			}
			totalRowCount = totalRowCount + regionRowCount;
		}

		return totalRowCount;
	}

	private Long wholeTableCountFast(final String tblName)
			throws ServiceException, Throwable {
		final AtomicLong totalRowCount = new AtomicLong();
		Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum", "10.111.32.203");
		HConnection connection = HConnectionManager.createConnection(config);
		HTableInterface table = connection.getTable(tblName);

		Batch.Call<RowCountService, RowCountResponse> callable = new Batch.Call<RowCountService, RowCountResponse>() {
			ServerRpcController controller = new ServerRpcController();
			BlockingRpcCallback<RowCountResponse> rpcCallback = new BlockingRpcCallback<RowCountResponse>();

			@Override
			public RowCountResponse call(RowCountService instance)
					throws IOException {
				RowCountRequest.Builder rcReqBuilder = RowCountRequest
						.newBuilder();
				RowCountParam.Builder rcParamBuilder = RowCountParam
						.newBuilder();
				rcParamBuilder.setKey("htab");
				rcParamBuilder.addVal(tblName);
				rcReqBuilder.addContext(rcParamBuilder);
				instance.getRowCount(controller, rcReqBuilder.build(),
						rpcCallback);
				return rpcCallback.get();
			}
		};
		Batch.Callback<RowCountResponse> callback = new Batch.Callback<RowCountResponse>() {
			@Override
			public void update(byte[] region, byte[] row,
					RowCountResponse result) {

				List<RowCountResult> rlist = result.getResultList();
				for (RowCountResult rcres : rlist) {

					List<String> lr = rcres.getValList();
					for (String r : lr) {
						totalRowCount.getAndAdd(Long.valueOf(r));
					}
					logger.info("totalRowCount={}", totalRowCount);
				}

			}
		};

		table.coprocessorService(RowCountService.class, null, null, callable,
				callback);
		return totalRowCount.get();
	}

}
