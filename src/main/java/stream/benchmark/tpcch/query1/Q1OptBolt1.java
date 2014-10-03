package stream.benchmark.tpcch.query1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.toolkits.MemoryReport;
import stream.benchmark.tpcch.query.TableState.ItemState;
import stream.benchmark.tpcch.query.TableState.DistrictState;
import stream.benchmark.tpcch.spout.BenchmarkConstant;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Q1OptBolt1 extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;

	private Connection _connection = null;
	private Statement _statement = null;
	private PreparedStatement _orderlinesInsertion;
	private PreparedStatement _orderlinesUpdate;
	private PreparedStatement _orderlinesQuery;

	private Map<Integer, ItemState> _itemsIndex;
	private Map<Integer, Map<Integer, DistrictState>> _districtsIndex;
	private Map<Integer, Map<Integer, List<Integer>>> _newordersIndex;

	private boolean _isFirstQuery = true;
	private long _beginTime;

	private int _numEventCount = 0;
	private int _currentOrderCount = 0;

	public void execute(Tuple input) {
		try {
			String tuple = input.getString(0);
			String[] fields = tuple.split(",");
			String streamname = input.getSourceStreamId();
			// item
			if (streamname == "item") {
				int item_id = Integer.valueOf(fields[0]);
				ItemState item = new ItemState(item_id,
						Integer.valueOf(fields[1]), fields[2],
						Double.valueOf(fields[3]), fields[4]);
				_itemsIndex.put(item_id, item);
			}

			// district
			else if (streamname == "district") {
				int district_id = Integer.valueOf(fields[0]);
				int warehouse_id = Integer.valueOf(fields[1]);
				DistrictState district = new DistrictState(district_id,
						warehouse_id, fields[2], fields[3], fields[4],
						fields[5], fields[6], fields[7],
						Double.valueOf(fields[8]), Double.valueOf(fields[9]),
						Integer.valueOf(fields[10]));
				if (!_districtsIndex.containsKey(warehouse_id)) {
					_districtsIndex.put(warehouse_id,
							new HashMap<Integer, DistrictState>());
				}
				_districtsIndex.get(warehouse_id).put(district_id, district);
			}

			// neworder
			else if (streamname == "neworder") {
				int order_id = Integer.valueOf(fields[0]);
				int district_id = Integer.valueOf(fields[1]);
				int warehouse_id = Integer.valueOf(fields[2]);
				if (!_newordersIndex.containsKey(warehouse_id)) {
					_newordersIndex.put(warehouse_id,
							new HashMap<Integer, List<Integer>>());
				}
				if (!_newordersIndex.get(warehouse_id).containsKey(district_id)) {
					_newordersIndex.get(warehouse_id).put(district_id,
							new LinkedList<Integer>());
				}
				_newordersIndex.get(warehouse_id).get(district_id)
						.add(order_id);
			}

			// orderline
			else if (streamname == "orderline") {
				_orderlinesInsertion.setInt(1, Integer.valueOf(fields[0]));
				_orderlinesInsertion.setInt(2, Integer.valueOf(fields[1]));
				_orderlinesInsertion.setInt(3, Integer.valueOf(fields[2]));
				_orderlinesInsertion.setInt(4, Integer.valueOf(fields[3]));
				_orderlinesInsertion.setInt(5, Integer.valueOf(fields[4]));
				_orderlinesInsertion.setInt(6, Integer.valueOf(fields[5]));
				_orderlinesInsertion.setLong(7, Long.valueOf(fields[6]));
				_orderlinesInsertion.setInt(8, Integer.valueOf(fields[7]));
				_orderlinesInsertion.setDouble(9, Double.valueOf(fields[8]));
				_orderlinesInsertion.setString(10, fields[9]);
				_orderlinesInsertion.addBatch();

			}

			else if (streamname == "DELIVERY") {
				int w_id = Integer.valueOf(fields[0]);
				// long ol_delivery_d = Long.valueOf(fields[2]);
				long ol_delivery_d = System.currentTimeMillis();
				// for each district, deliver the first new_order
				for (int d_id = 1; d_id < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE + 1; ++d_id) {
					// /////////////////////////////////////////////////////////////
					// randomly pick a new order id if exists, an delete it from
					// NewOrderList.
					if (_newordersIndex.get(w_id).get(d_id).size() == 0) {
						continue;
					}
					int no_o_id = _newordersIndex.get(w_id).get(d_id).remove(0);

					// ///////////////////////////////////////////////////////////////
					// updateOrderLine : ol_delivery_d, no_o_id, d_id, w_id

					_orderlinesUpdate.setLong(1, ol_delivery_d);
					_orderlinesUpdate.setInt(2, w_id);
					_orderlinesUpdate.setInt(3, d_id);
					_orderlinesUpdate.setInt(4, no_o_id);
					_orderlinesUpdate.executeUpdate();
				}
				// event count
				_numEventCount += 1;
			}

			else if (streamname == "NEW_ORDER") {
				int w_id = Integer.valueOf(fields[0]);
				int d_id = Integer.valueOf(fields[1]);
				long o_entry_d = Long.valueOf(fields[3]);
				List<Integer> i_ids = new LinkedList<Integer>();
				for (String tmp : fields[4].split(";")) {
					i_ids.add(Integer.valueOf(tmp));
				}
				List<Integer> i_w_ids = new LinkedList<Integer>();
				for (String tmp : fields[5].split(";")) {
					i_w_ids.add(Integer.valueOf(tmp));
				}
				List<Integer> i_qtys = new LinkedList<Integer>();
				for (String tmp : fields[6].split(";")) {
					i_qtys.add(Integer.valueOf(tmp));
				}

				// getDistrict : d_id, w_id
				int d_next_o_id = _districtsIndex.get(w_id).get(d_id)._next_o_id;
				_districtsIndex.get(w_id).get(d_id)._next_o_id = d_next_o_id + 1;

				for (int i = 0; i < i_ids.size(); ++i) {
					int ol_number = i + 1;
					int ol_supply_w_id = i_w_ids.get(i);
					int ol_i_id = i_ids.get(i);
					int ol_quantity = i_qtys.get(i);
					double ol_amount = ol_quantity
							* _itemsIndex.get(i_ids.get(i))._price;
					// create new order line
					_orderlinesInsertion.setInt(1, d_next_o_id);
					_orderlinesInsertion.setInt(2, d_id);
					_orderlinesInsertion.setInt(3, w_id);
					_orderlinesInsertion.setInt(4, ol_number);
					_orderlinesInsertion.setInt(5, ol_i_id);
					_orderlinesInsertion.setInt(6, ol_supply_w_id);
					_orderlinesInsertion.setLong(7, o_entry_d);
					_orderlinesInsertion.setInt(8, ol_quantity);
					_orderlinesInsertion.setDouble(9, ol_amount);
					_orderlinesInsertion.setString(10, "dist_info");
					_orderlinesInsertion.addBatch();
				}
				_orderlinesInsertion.executeBatch();

				if (!_newordersIndex.containsKey(w_id)) {
					_newordersIndex.put(w_id,
							new HashMap<Integer, List<Integer>>());
				}
				if (!_newordersIndex.get(w_id).containsKey(d_id)) {
					_newordersIndex.get(w_id).put(d_id,
							new LinkedList<Integer>());
				}
				_newordersIndex.get(w_id).get(d_id).add(d_next_o_id);

				// event count
				++_numEventCount;
				_currentOrderCount = d_next_o_id;
			}

			if (streamname == "DELIVERY" || streamname == "NEW_ORDER") {
				if (_isFirstQuery) {
					_orderlinesInsertion.executeBatch();
					long elapsedTime = System.currentTimeMillis() - _beginTime;
					System.out.println("load database elapsed time = "
							+ elapsedTime + "ms");
					_isFirstQuery = false;
					_beginTime = System.currentTimeMillis();

				} else if (_numEventCount % 20000 == 0) {
					_numEventCount = 0;
					System.out
							.println("################################################");
					System.out.println("elapsed consume time = "
							+ (System.currentTimeMillis() - _beginTime) + "ms");

					// /////////////////////////////////////////////////////////////////
					long startQueryTime = System.currentTimeMillis();

					_orderlinesQuery.setInt(1, _currentOrderCount);
					ResultSet results = _orderlinesQuery.executeQuery();
					while (results.next()) {
						int warehouse = results.getInt(1);
						int district = results.getInt(2);
						int quantity_sum = results.getInt(3);
						float amount_sum = results.getFloat(4);
						int quantity_avg = results.getInt(5);
						float amount_avg = results.getFloat(6);
						int count = results.getInt(7);
						StringBuilder sb = new StringBuilder();

						sb.append(warehouse);
						sb.append(", ");
						sb.append(district);
						sb.append(", ");
						sb.append(quantity_sum);
						sb.append(", ");
						sb.append(amount_sum);
						sb.append(", ");
						sb.append(quantity_avg);
						sb.append(", ");
						sb.append(amount_avg);
						sb.append(", ");
						sb.append(count);
						_collector.emit(new Values(sb.toString()));
						sb.setLength(0);
						System.out.println("count="+count);
					}

					System.out.println("elapsed query time = "
							+ (System.currentTimeMillis() - startQueryTime)
							+ "ms");

					MemoryReport.reportStatus();

					_beginTime = System.currentTimeMillis();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;

		_itemsIndex = new HashMap<Integer, ItemState>();
		_districtsIndex = new HashMap<Integer, Map<Integer, DistrictState>>();
		_newordersIndex = new HashMap<Integer, Map<Integer, List<Integer>>>();

		databaseInit();

		_beginTime = System.currentTimeMillis();
	}

	protected void databaseInit() {
		try {
			_connection = DriverManager.getConnection("jdbc:sqlite::memory:");
			_statement = _connection.createStatement();
			
			// orderlines
			_statement
					.executeUpdate("create table orderlines"
							+ "(ol_o_id int, ol_d_id smallint, ol_w_id smallint, "
							+ "ol_number int, ol_i_id int, ol_supply_w_id smallint, "
							+ "ol_delivery_d bigint, ol_quantity int, ol_amount float, ol_dist_info varchar(32))");
			_statement
					.executeUpdate("create index orderlinesindex on orderlines(ol_w_id, ol_d_id, ol_o_id)");
			
			_orderlinesInsertion = _connection
					.prepareStatement("insert into orderlines values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			_orderlinesUpdate = _connection
					.prepareStatement("update orderlines set ol_delivery_d=? where ol_w_id=? and ol_d_id=? and ol_o_id=?");
			_orderlinesQuery = _connection
					.prepareStatement("select ol_w_id, ol_d_id, "
							+ "sum(ol_quantity) as sum_qty, sum(ol_amount) as sum_amount, "
							+ "avg(ol_quantity) as avg_qty, avg(ol_amount) as avg_amount, "
							+ "count(*) as count_order " + "from orderlines "
							+ "where ol_o_id > ? - 2000 "
							+ "group by ol_w_id, ol_d_id");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
