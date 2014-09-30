package stream.benchmark.tpcch.query1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.tpcch.query1.Q1State.OrderLineState;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Q1HeapBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;

	private Map<Integer, Map<Integer, List<OrderLineState>>> _orderlinesTree;

	private Connection _connection = null;
	private Statement _statement = null;
	private Statement _orderlinesInsertion;

	private long _beginTime;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname == "orderline") {
			int order_id = Integer.valueOf(fields[0]);
			int district_id = Integer.valueOf(fields[1]);
			int warehouse_id = Integer.valueOf(fields[2]);
			OrderLineState orderline = new OrderLineState(order_id,
					district_id, warehouse_id, Integer.valueOf(fields[3]),
					Integer.valueOf(fields[4]), Integer.valueOf(fields[5]),
					Long.valueOf(fields[6]), Integer.valueOf(fields[7]),
					Double.valueOf(fields[8]), fields[9]);
			if (!_orderlinesTree.containsKey(warehouse_id)) {
				_orderlinesTree.put(warehouse_id,
						new HashMap<Integer, List<OrderLineState>>());
			}
			if (!_orderlinesTree.get(warehouse_id).containsKey(district_id)) {
				_orderlinesTree.get(warehouse_id).put(district_id,
						new LinkedList<OrderLineState>());
			}
			_orderlinesTree.get(warehouse_id).get(district_id).add(orderline);

			// if (!_orderlinesIndex.containsKey(warehouse_id)) {
			// _orderlinesIndex
			// .put(warehouse_id,
			// new HashMap<Integer, Map<Integer, List<OrderLineState>>>());
			// }
			// if (!_orderlinesIndex.get(warehouse_id)
			// .containsKey(district_id)) {
			// _orderlinesIndex.get(warehouse_id).put(district_id,
			// new HashMap<Integer, List<OrderLineState>>());
			// }
			// if (!_orderlinesIndex.get(warehouse_id).get(district_id)
			// .containsKey(order_id)) {
			// _orderlinesIndex.get(warehouse_id).get(district_id)
			// .put(order_id, new LinkedList<OrderLineState>());
			// }
			// _orderlinesIndex.get(warehouse_id).get(district_id)
			// .get(order_id).add(orderline);

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
			// _orderlinesInsertion.addBatch();
			// _orderlinesInsertion.executeUpdate();
		}

		if (streamname == "DELIVERY" || streamname == "NEW_ORDER"
				|| streamname == "ORDER_STATUS" || streamname == "PAYMENT"
				|| streamname == "STOCK_LEVEL") {
			if (System.currentTimeMillis() - _beginTime >= 2000) {
				StringBuilder sb = new StringBuilder();
				for (Integer warehouse : _orderlinesTree.keySet()) {
					for (Integer district : _orderlinesTree.get(warehouse)
							.keySet()) {
						int quantity_num = 0;
						int quantity_sum = 0;
						int amount_num = 0;
						int amount_sum = 0;
						for (OrderLineState state : _orderlinesTree.get(
								warehouse).get(district)) {
							if (System.currentTimeMillis()
									- state._ol_delivery_d < 10000) {
								quantity_num += 1;
								quantity_sum += state._ol_quantity;
								amount_num += 1;
								amount_sum += state._ol_amount;
							}
						}
						sb.append(warehouse);
						sb.append(", ");
						sb.append(district);
						sb.append(", ");
						sb.append(quantity_sum);
						sb.append(", ");
						if (quantity_num == 0) {
							sb.append(0);
						} else {
							sb.append(quantity_sum / quantity_num);
						}
						sb.append(", ");
						sb.append(amount_sum);
						sb.append(", ");
						if (amount_num == 0) {
							sb.append(0);
						} else {
							sb.append(amount_sum / amount_num);
						}
						_collector.emit(new Values(sb.toString()));
						sb.setLength(0);
					}
				}
				System.out.println("trigger");
				_beginTime = System.currentTimeMillis();
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
		_orderlinesTree = new HashMap<Integer, Map<Integer, List<OrderLineState>>>();
		databaseInit();
		_beginTime = System.currentTimeMillis();
	}

	protected void databaseInit() {
		try {
			_connection = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/tpcc", "root", "");
			_statement = _connection.createStatement();

			_orderlinesInsertion = _connection
					.prepareStatement("insert into orderlines values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

			System.out.println("database initiated!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
