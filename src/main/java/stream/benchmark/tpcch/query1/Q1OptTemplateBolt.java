package stream.benchmark.tpcch.query1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.toolkits.MemoryReport;
import stream.benchmark.tpcch.query.TableState.ItemState;
import stream.benchmark.tpcch.query.TableState.OrderLineState;
import stream.benchmark.tpcch.query.TableState.DistrictState;
import stream.benchmark.tpcch.spout.BenchmarkConstant;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Q1OptTemplateBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;

	private Connection _connection = null;
	private Statement _statement = null;
	private PreparedStatement _itemsInsertion;
	private PreparedStatement _districtsInsertion;
	private PreparedStatement _newordersInsertion;
	private PreparedStatement _orderlinesInsertion;
	private PreparedStatement _orderlinesUpdate;

	private Map<Integer, ItemState> _itemsIndex;
	private Map<Integer, Map<Integer, DistrictState>> _districtsIndex;
	private Map<Integer, Map<Integer, List<Integer>>> _newordersIndex;
	private Map<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>> _orderlinesIndex;

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

				// _itemsInsertion.setInt(1, Integer.valueOf(fields[0]));
				// _itemsInsertion.setInt(2, Integer.valueOf(fields[1]));
				// _itemsInsertion.setString(3, fields[2]);
				// _itemsInsertion.setDouble(4, Double.valueOf(fields[3]));
				// _itemsInsertion.setString(5, fields[4]);
				// _itemsInsertion.executeUpdate();
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

				// _districtsInsertion.setInt(1, Integer.valueOf(fields[0]));
				// _districtsInsertion.setInt(2, Integer.valueOf(fields[1]));
				// _districtsInsertion.setString(3, fields[2]);
				// _districtsInsertion.setString(4, fields[3]);
				// _districtsInsertion.setString(5, fields[4]);
				// _districtsInsertion.setString(6, fields[5]);
				// _districtsInsertion.setString(7, fields[6]);
				// _districtsInsertion.setString(8, fields[7]);
				// _districtsInsertion.setDouble(9, Double.valueOf(fields[8]));
				// _districtsInsertion.setDouble(10, Double.valueOf(fields[9]));
				// _districtsInsertion.setInt(11, Integer.valueOf(fields[10]));
				// _districtsInsertion.executeUpdate();
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

				// _newordersInsertion.setInt(1, Integer.valueOf(fields[0]));
				// _newordersInsertion.setInt(2, Integer.valueOf(fields[1]));
				// _newordersInsertion.setInt(3, Integer.valueOf(fields[2]));
				// _newordersInsertion.executeUpdate();
			}

			// orderline
			else if (streamname == "orderline") {
				int order_id = Integer.valueOf(fields[0]);
				int district_id = Integer.valueOf(fields[1]);
				int warehouse_id = Integer.valueOf(fields[2]);
				OrderLineState orderline = new OrderLineState(order_id,
						district_id, warehouse_id, Integer.valueOf(fields[3]),
						Integer.valueOf(fields[4]), Integer.valueOf(fields[5]),
						Long.valueOf(fields[6]), Integer.valueOf(fields[7]),
						Double.valueOf(fields[8]), fields[9]);
				if (!_orderlinesIndex.containsKey(warehouse_id)) {
					_orderlinesIndex
							.put(warehouse_id,
									new HashMap<Integer, Map<Integer, List<OrderLineState>>>());
				}
				if (!_orderlinesIndex.get(warehouse_id)
						.containsKey(district_id)) {
					_orderlinesIndex.get(warehouse_id).put(district_id,
							new HashMap<Integer, List<OrderLineState>>());
				}
				if (!_orderlinesIndex.get(warehouse_id).get(district_id)
						.containsKey(order_id)) {
					_orderlinesIndex.get(warehouse_id).get(district_id)
							.put(order_id, new LinkedList<OrderLineState>());
				}
				_orderlinesIndex.get(warehouse_id).get(district_id)
						.get(order_id).add(orderline);

				// _orderlinesInsertion.setInt(1, Integer.valueOf(fields[0]));
				// _orderlinesInsertion.setInt(2, Integer.valueOf(fields[1]));
				// _orderlinesInsertion.setInt(3, Integer.valueOf(fields[2]));
				// _orderlinesInsertion.setInt(4, Integer.valueOf(fields[3]));
				// _orderlinesInsertion.setInt(5, Integer.valueOf(fields[4]));
				// _orderlinesInsertion.setInt(6, Integer.valueOf(fields[5]));
				// _orderlinesInsertion.setLong(7, Long.valueOf(fields[6]));
				// _orderlinesInsertion.setInt(8, Integer.valueOf(fields[7]));
				// _orderlinesInsertion.setDouble(9, Double.valueOf(fields[8]));
				// _orderlinesInsertion.setString(10, fields[9]);
				// _orderlinesInsertion.executeUpdate();
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
					for (OrderLineState tmp : _orderlinesIndex.get(w_id)
							.get(d_id).get(no_o_id)) {
						tmp._ol_delivery_d = ol_delivery_d;
					}

					// _orderlinesUpdate.setLong(1, ol_delivery_d);
					// _orderlinesUpdate.setInt(2, w_id);
					// _orderlinesUpdate.setInt(3, d_id);
					// _orderlinesUpdate.setInt(4, no_o_id);
					// _orderlinesUpdate.executeUpdate();
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
					OrderLineState olState = new OrderLineState(d_next_o_id,
							d_id, w_id, ol_number, ol_i_id, ol_supply_w_id,
							o_entry_d, ol_quantity, ol_amount, "dist_info");
					if (!_orderlinesIndex.get(w_id).get(d_id)
							.containsKey(d_next_o_id)) {
						_orderlinesIndex
								.get(w_id)
								.get(d_id)
								.put(d_next_o_id,
										new LinkedList<OrderLineState>());
					}
					_orderlinesIndex.get(w_id).get(d_id).get(d_next_o_id)
							.add(olState);
				}

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
					StringBuilder sb = new StringBuilder();
					for (Integer warehouse : _orderlinesIndex.keySet()) {
						for (Integer district : _orderlinesIndex.get(warehouse)
								.keySet()) {
							int quantity_sum = 0;
							int amount_sum = 0;
							int orderline_num = 0;
							// get order
							for (int order : _orderlinesIndex.get(warehouse)
									.get(district).keySet()) {
								// get orderlines
								for (OrderLineState orderline : _orderlinesIndex
										.get(warehouse).get(district)
										.get(order)) {
									if (_currentOrderCount - orderline._ol_o_id < 2000) {
										quantity_sum += orderline._ol_quantity;
										amount_sum += orderline._ol_amount;
										++orderline_num;
									}
								}
							}
							sb.append(warehouse);
							sb.append(", ");
							sb.append(district);
							sb.append(", ");
							sb.append(quantity_sum);
							sb.append(", ");
							if (orderline_num == 0) {
								sb.append(0);
							} else {
								sb.append(quantity_sum / orderline_num);
							}
							sb.append(", ");
							sb.append(amount_sum);
							sb.append(", ");
							if (orderline_num == 0) {
								sb.append(0);
							} else {
								sb.append(amount_sum / orderline_num);
							}
							sb.append(", ");
							sb.append(orderline_num);
							_collector.emit(new Values(sb.toString()));
							System.out
									.println("orderline num=" + orderline_num);
							sb.setLength(0);
						}
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
		_orderlinesIndex = new HashMap<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>>();

		databaseInit();

		_beginTime = System.currentTimeMillis();
	}

	protected void databaseInit() {
		try {
			_connection = DriverManager.getConnection("jdbc:sqlite::memory:");
			_statement = _connection.createStatement();
			// items
			_statement
					.executeUpdate("create table items"
							+ "(i_id int, i_im_id int, i_name varchar(32), i_price float, i_data varchar(64))");
			_statement.executeUpdate("create index itemsindex on items(i_id)");
			// districts
			_statement
					.executeUpdate("create table districts"
							+ "(d_id smallint, d_w_id smallint, d_name varchar(16), "
							+ "d_street_1 varchar(32), d_street_2 varchar(32), d_city varchar(32), d_state varchar(2), d_zip varchar(9), "
							+ "d_tax float, d_ytd float, d_next_o_id int)");
			_statement
					.executeUpdate("create index districtsindex on districts(d_w_id, d_id)");
			// neworders
			_statement.executeUpdate("create table neworders"
					+ "(no_o_id int, no_d_id smallint, no_w_id smallint)");
			_statement
					.executeUpdate("create index newordersindex on neworders(no_d_id, no_w_id, no_o_id)");
			// orderlines
			_statement
					.executeUpdate("create table orderlines"
							+ "(ol_o_id int, ol_d_id smallint, ol_w_id smallint, "
							+ "ol_number int, ol_i_id int, ol_supply_w_id smallint, "
							+ "ol_delivery_d bigint, ol_quantity int, ol_amount float, ol_dist_info varchar(32))");
			_statement
					.executeUpdate("create index orderlinesindex on orderlines(ol_w_id, ol_d_id, ol_o_id, ol_number)");

			_itemsInsertion = _connection
					.prepareStatement("insert into items values(?, ?, ?, ?, ?)");
			_districtsInsertion = _connection
					.prepareStatement("insert into districts values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			_newordersInsertion = _connection
					.prepareStatement("insert into neworders values(?, ?, ?)");
			_orderlinesInsertion = _connection
					.prepareStatement("insert into orderlines values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

			_orderlinesUpdate = _connection
					.prepareStatement("update orderlines set ol_delivery_d=? where ol_w_id=? and ol_d_id=? and ol_o_id=?");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
