/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年4月19日
 * @Copyright ZHL All Rights Reserved.
 */
package zhl.study.hbaseapp.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.slf4j.LoggerFactory;

public class RowCountObserver extends BaseRegionObserver {
	private static final org.slf4j.Logger logger = LoggerFactory
			.getLogger(RowCountObserver.class);

	RegionCoprocessorEnvironment env;

	private HRegion hRegion;

	private boolean initcount = false;

	private String zNodePath = "/hbase/hbase-app/demo";
	private ZooKeeperWatcher zkw = null;
	private Long myRowCount = 0L;

	@Override
	public void start(CoprocessorEnvironment envi) throws IOException {

		env = (RegionCoprocessorEnvironment) envi;
		RegionCoprocessorEnvironment re = (RegionCoprocessorEnvironment) envi;
		RegionServerServices rss = re.getRegionServerServices();
		hRegion = re.getRegion();
		zNodePath = zNodePath + hRegion.getRegionNameAsString();
		zkw = rss.getZooKeeper();
		myRowCount = 0L; // count;
		initcount = false;

		try {
			if (ZKUtil.checkExists(zkw, zNodePath) == -1) {
				logger.error("LIULIUMI: cannot find the znode");
				ZKUtil.createWithParents(zkw, zNodePath);
				logger.info("znode path is : " + zNodePath);
			}
		} catch (Exception ee) {
			logger.error("LIULIUMI: create znode failed");
		}
	}

	@Override
	public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
		logger.info("postOpen已经进入了...");
		long count = 0;
		try {
			if (initcount == false) {
				Scan scan = new Scan();
				InternalScanner scanner = null;
				scanner = hRegion.getScanner(scan);
				List<Cell> results = new ArrayList<Cell>();
				boolean hasMore = false;
				do {
					hasMore = scanner.next(results);
					if (results.size() > 0)
						count++;
				} while (hasMore);
				
				initcount = true;
			}
			logger.info("postOpen-open invoked");
			ZKUtil.setData(zkw, zNodePath, Bytes.toBytes(count));
		} catch (Exception ex) {
			logger.info("setData exception");
		}

	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {

	}

	@Override
	public void preDelete(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Delete delete, final WALEdit edit, final Durability durability)
			throws IOException {
		logger.info("preDelete已经进入了...");
		
		myRowCount--;
		logger.info("preDelete,regionName={},myRowCount={}",e.getEnvironment().getRegion().getRegionNameAsString(),myRowCount);
		try {
			ZKUtil.setData(zkw, zNodePath, Bytes.toBytes(myRowCount));
		} catch (Exception ex) {
			logger.info("preDelete-setData-exception={}", ex);
		}

	}

	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Put put, final WALEdit edit, final Durability durability)
			throws IOException {
		logger.info("prePut已经进入了...");
		
		myRowCount++;
		logger.info("prePut,regionName={},myRowCount={}",e.getEnvironment().getRegion().getRegionNameAsString(),myRowCount);
		try {
			ZKUtil.setData(zkw, zNodePath, Bytes.toBytes(myRowCount));
		} catch (Exception ex) {
			logger.info("prePut-setData-exception={}", ex);
		}
	}

}
