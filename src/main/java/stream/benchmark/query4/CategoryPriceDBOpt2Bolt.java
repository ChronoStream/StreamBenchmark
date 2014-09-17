package stream.benchmark.query4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CategoryPriceDBOpt2Bolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private int emitCount = 0;
	private final long slidingInterval = 100000;
	private long firstPoint = 0;
	private long secondPoint = slidingInterval;

	private OutputCollector _collector;

	private Connection connection = null;
	private Statement statement = null;
	private PreparedStatement auctionInsertion = null;
	private PreparedStatement auctionTmpInsertion = null;
	private PreparedStatement bidInsertion = null;
	private PreparedStatement joinTables = null;
	
	private long measureBeginTime, measureElapsedTime;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname == "auction") {
			// schema: auction_id, seller_id, category_id, begin_time, end_time
			String auction_id = fields[0];
			int category = Integer.valueOf(fields[2]);
			long begin_time = Long.valueOf(fields[3]);
			long end_time = Long.valueOf(fields[4]);
			
			try{
				auctionInsertion.setString(1, auction_id);
				auctionInsertion.setInt(2, category);
				auctionInsertion.setLong(3, begin_time);
				auctionInsertion.setLong(4, end_time);
				auctionInsertion.addBatch();
			}catch(Exception e){
				e.printStackTrace();
			}
		} else if (streamname == "bid") {
			// schema: auction_id, date_time, person_id, price
			String auction_id = fields[0];
			long date_time = Long.valueOf(fields[1]);
			float price = Float.valueOf(fields[3]);
			
			try{
				bidInsertion.setString(1, auction_id);
				bidInsertion.setLong(2, date_time);
				bidInsertion.setFloat(3, price);
				bidInsertion.addBatch();
			}catch(Exception e){
				e.printStackTrace();
			}
			
			if (date_time > secondPoint) {
				try {
					auctionInsertion.executeBatch();
					bidInsertion.executeBatch();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				processQuery();
				firstPoint += slidingInterval;
				secondPoint += slidingInterval;
				emitCount += 1;
				measureElapsedTime=System.currentTimeMillis()-measureBeginTime;
				System.out.println("elapsed time="+measureElapsedTime+"ms");
				measureBeginTime=System.currentTimeMillis();
			}
		}
	}

	protected class SimplePriceCount {
		public SimplePriceCount() {
			price = 0;
			count = 0;
		}

		void addPrice(float p) {
			price += p;
			count += 1;
		}

		float computeAverage() {
			return price / count;
		}

		float price;
		int count;
	}

	protected void processQuery() {
		try{
			joinTables.setLong(1, firstPoint);
			ResultSet result = joinTables.executeQuery();
			while (result.next()){
				int res_category = result.getInt(1);
				float res_average_price = result.getFloat(2);
				_collector.emit(new Values(res_category, res_average_price, emitCount));
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
		databaseInit();
		measureBeginTime = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("category", "price", "emitcount"));
	}

	protected void databaseInit() {
		try {
			connection = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/testdb", "root", "");
			statement = connection.createStatement();
			statement.executeUpdate("drop table auctiontable");
			statement.executeUpdate("drop table bidtable");
			
			statement.executeUpdate("create table auctiontable"
					+ "(auction_id varchar(20), category int, begin_time bigint, end_time bigint) "
					+ "engine=memory");
			statement.executeUpdate("create index auctionindex on auctiontable(auction_id)");
			
			statement.executeUpdate("create table bidtable"
					+ "(auction_id varchar(20), date_time bigint, price float) "
					+ "engine=memory");
			statement.executeUpdate("create index bidindex on bidtable(auction_id)");
			
			auctionInsertion = connection.prepareStatement("insert into auctiontable values(?, ?, ?, ?)");
			bidInsertion = connection.prepareStatement("insert into bidtable values(?, ?, ?)");
			joinTables = connection.prepareStatement("select auctiontable.category, avg(maxbidtable.maxprice) "
					+ "from "
					+ "auctiontable "
					+ "inner join "
					+ "((select auction_id, max(price) as maxprice from bidtable where date_time>? group by auction_id) as maxbidtable) "
					+ "where "
					+ "auctiontable.auction_id=maxbidtable.auction_id "
					+ "group by "
					+ "auctiontable.category");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
