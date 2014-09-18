package stream.benchmark.nexmark.query5;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import stream.benchmark.nexmark.query5.HotItemsState.AuctionInfo;
import stream.benchmark.nexmark.query5.HotItemsState.BidInfo;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HotItemsDBBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private int emitCount = 0;
	private final long slidingInterval = 10000;
	private long firstPoint = 0;
	private long secondPoint = slidingInterval;

	private OutputCollector _collector;

	private Connection connection = null;
	private Statement statement = null;
	private PreparedStatement auctionInsertion = null;
	private PreparedStatement bidInsertion = null;
	private PreparedStatement joinTables = null;

	Map<String, AuctionInfo> auctionMap = new HashMap<String, AuctionInfo>();
	Map<String, List<BidInfo>> bidMap = new HashMap<String, List<BidInfo>>();

	private long measureBeginTime, measureElapsedTime;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname == "auction") {
			// schema: auction_id, seller_id, category_id, begin_time, end_time
			String auction_id = fields[0];
			String seller_id = fields[1];
			long begin_time = Long.valueOf(fields[3]);
			long end_time = Long.valueOf(fields[4]);

			try {
				auctionInsertion.setString(1, auction_id);
				auctionInsertion.setString(2, seller_id);
				auctionInsertion.setLong(3, begin_time);
				auctionInsertion.setLong(4, end_time);
				auctionInsertion.executeUpdate();
			} catch (Exception e) {
				e.printStackTrace();
			}

		} else if (streamname == "bid") {
			// schema: auction_id, date_time, person_id, price
			String auction_id = fields[0];
			long date_time = Long.valueOf(fields[1]);
			float price = Float.valueOf(fields[3]);

			try {
				bidInsertion.setString(1, auction_id);
				bidInsertion.setLong(2, date_time);
				bidInsertion.setFloat(3, price);
				bidInsertion.executeUpdate();
			} catch (Exception e) {
				e.printStackTrace();
			}

			if (date_time > secondPoint) {
				processQuery();
				firstPoint += slidingInterval;
				secondPoint += slidingInterval;
				emitCount += 1;
				measureElapsedTime = System.currentTimeMillis()
						- measureBeginTime;
				System.out.println("elapsed time=" + measureElapsedTime + "ms");
				measureBeginTime = System.currentTimeMillis();
			}
		}
	}

	protected void processQuery() {
		int hotCount = -1;
		String hotId = "";
		for (String auction_id : bidMap.keySet()) {
			int count = 0;
			List<BidInfo> tmpBidList = bidMap.get(auction_id);
			for (BidInfo tmpBid : tmpBidList) {
				if (tmpBid.date_time > firstPoint) {
					count += 1;
				}
			}
			if (count > hotCount) {
				hotCount = count;
				hotId = auction_id;
			}
		}
		if (hotCount != -1) {
			List<BidInfo> tmpBidList = bidMap.get(hotId);
			float maxPrice = -1;
			for (BidInfo tmpBid : tmpBidList) {
				if (tmpBid.date_time > firstPoint && tmpBid.price > maxPrice) {
					maxPrice = tmpBid.price;
				}
			}
			String seller_id = auctionMap.get(hotId).seller_id;
			_collector.emit(new Values(hotId, seller_id, hotCount, maxPrice,
					emitCount));
		}

		try {
			joinTables.setLong(1, firstPoint);
			ResultSet result = joinTables.executeQuery();
			while (result.next()) {
				String res_auction_id = result.getString(1);
				String res_seller_id = result.getString(2);
				int res_hot_count = result.getInt(3);
				float res_max_price = result.getFloat(4);
				_collector.emit(new Values(res_auction_id, res_seller_id,
						res_hot_count, res_max_price, emitCount));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void databaseInit() {
		try {
			connection = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/testdb", "root", "");
			statement = connection.createStatement();
			statement.executeUpdate("drop table auctiontable");
			statement.executeUpdate("drop table bidtable");

			statement
					.executeUpdate("create table auctiontable"
							+ "(auction_id varchar(20), seller_id varchar(20), begin_time bigint, end_time bigint) "
							+ "engine=memory");

			statement
					.executeUpdate("create table bidtable"
							+ "(auction_id varchar(20), date_time bigint, price float) "
							+ "engine=memory");

			auctionInsertion = connection
					.prepareStatement("insert into auctiontable values(?, ?, ?, ?)");
			bidInsertion = connection
					.prepareStatement("insert into bidtable values(?, ?, ?)");
			joinTables = connection.prepareStatement("select * from bidtable");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
		databaseInit();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("auction_id", "seller_id", "bidcount",
				"price", "emitcount"));
	}

}
