package stream.benchmark.nexmark.query3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SuggestionDBBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private int emitCount = 0;
	private final long slidingInterval = 100000;
	private long firstPoint = 0;
	private long secondPoint = slidingInterval;

	OutputCollector _collector;

	private Connection connection = null;
	private Statement statement = null;
	private PreparedStatement personInsertion = null;
	private PreparedStatement auctionInsertion = null;
	private PreparedStatement joinTables = null;
	
	private long measureBeginTime, measureElapsedTime;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname == "auction") {
			// schema: auction_id, seller_id, category_id, begin_time, end_time
			String auction_id = fields[0];
			String seller_id = fields[1];
			int category = Integer.valueOf(fields[2]);
			long begin_time = Long.valueOf(fields[3]);
			try {
				auctionInsertion.setString(1, seller_id);
				auctionInsertion.setString(2, auction_id);
				auctionInsertion.setInt(3, category);
				auctionInsertion.setLong(4, begin_time);
				auctionInsertion.addBatch();
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (begin_time > secondPoint) {
				processQuery();
				firstPoint += slidingInterval;
				secondPoint += slidingInterval;
				emitCount += 1;
				measureElapsedTime=System.currentTimeMillis()-measureBeginTime;
				System.out.println("elapsed time="+measureElapsedTime+"ms");
				measureBeginTime=System.currentTimeMillis();
			}
		} else if (streamname == "person") {
			// schema: person_id, street_name, email, city, state, country
			String person_id = fields[0];
			String city = fields[3];
			String state = fields[4];
			String country = fields[5];
			try {
				personInsertion.setString(1, person_id);
				personInsertion.setString(2, city);
				personInsertion.setString(3, state);
				personInsertion.setString(4, country);
				personInsertion.addBatch();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	protected void processQuery() {
		try {
			auctionInsertion.executeBatch();
			personInsertion.executeBatch();
			joinTables.setLong(1, firstPoint);
			ResultSet result = joinTables.executeQuery();
			// person_id, auction_id, city, state, country, category
			while (result.next()) {
				String res_person_id = result.getString(1);
				String res_auction_id = result.getString(2);
				String res_city = result.getString(3);
				String res_state = result.getString(4);
				String res_country = result.getString(5);
				int res_category = result.getInt(6);
				_collector.emit(new Values(res_person_id,
						res_auction_id, res_city, res_state,
						res_country, res_category, emitCount));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
		databaseInit();
		measureBeginTime=System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("person_id", "auction_id", "city", "state",
				"country", "category", "emitcount"));
	}

	protected void databaseInit() {
		try {
			connection = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/testdb", "root", "");
			statement = connection.createStatement();
			statement.executeUpdate("drop table persontable");
			statement.executeUpdate("drop table auctiontable");
			statement
					.executeUpdate("create table persontable"
							+ "(person_id varchar(20), city varchar(20), province varchar(20), country varchar(30)) "
							+ "engine=memory");
			statement.executeUpdate("create index personindex on persontable(person_id)");
			statement
					.executeUpdate("create table auctiontable"
							+ "(seller varchar(20), auction_id varchar(20), category int, begin_time bigint) "
							+ "engine=memory");
			statement
					.executeUpdate("create index auctionindex on auctiontable(seller)");

			personInsertion = connection
					.prepareStatement("insert into persontable values(?, ?, ?, ?)");
			auctionInsertion = connection
					.prepareStatement("insert into auctiontable values(?, ?, ?, ?)");
			joinTables = connection
					.prepareStatement("select person_id, auction_id, city, province, country, category "
							+ "from "
							+ "persontable inner join auctiontable "
							+ "where "
							+ "persontable.country='United States' "
							+ "and "
							+ "persontable.person_id=auctiontable.seller "
							+ "and "
							+ "auctiontable.begin_time>? "
							+ "and "
							+ "auctiontable.category%10=0");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
