package stream.benchmark.tpcch.query3;

import java.util.HashMap;
import java.util.Map;

import stream.benchmark.tpcch.query3.Q3State.OrderState;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class Q3HeapBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;

	long _beginTime;

	// warehouse_id -> district_id -> order_id -> <customer_id, list<price>>
	Map<Integer, Map<Integer, Map<Integer, OrderState>>> _orderTree;

	// warehouse -> district -> customer_id -> customer_province
	Map<Integer, Map<Integer, Map<Integer, String>>> _customerTree;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname.equals("neworder")) {
			int order_id = Integer.valueOf(fields[0]);
			int district_id = Integer.valueOf(fields[1]);
			int warehouse_id = Integer.valueOf(fields[2]);
			if (!_orderTree.containsKey(warehouse_id)) {
				_orderTree.put(warehouse_id,
						new HashMap<Integer, Map<Integer, OrderState>>());
			}
			if (!_orderTree.get(warehouse_id).containsKey(district_id)) {
				_orderTree.get(warehouse_id).put(district_id,
						new HashMap<Integer, OrderState>());
			}
			if (!_orderTree.get(warehouse_id).get(district_id)
					.containsKey(order_id)) {
				_orderTree.get(warehouse_id).get(district_id)
						.put(order_id, new OrderState());
			}
			_orderTree.get(warehouse_id).get(district_id).get(order_id)._is_new = true;

		} else if (streamname.equals("order")) {
			int order_id = Integer.valueOf(fields[0]);
			int customer_id = Integer.valueOf(fields[1]);
			int district_id = Integer.valueOf(fields[2]);
			int warehouse_id = Integer.valueOf(fields[3]);
			if (!_orderTree.containsKey(warehouse_id)) {
				_orderTree.put(warehouse_id,
						new HashMap<Integer, Map<Integer, OrderState>>());
			}
			if (!_orderTree.get(warehouse_id).containsKey(district_id)) {
				_orderTree.get(warehouse_id).put(district_id,
						new HashMap<Integer, OrderState>());
			}
			if (!_orderTree.get(warehouse_id).get(district_id)
					.containsKey(order_id)) {
				_orderTree.get(warehouse_id).get(district_id)
						.put(order_id, new OrderState());
			}
			_orderTree.get(warehouse_id).get(district_id).get(order_id)._customer_id = customer_id;
			_orderTree.get(warehouse_id).get(district_id).get(order_id)._o_entry_d = Long
					.valueOf(fields[4]);

		} else if (streamname.equals("orderline")) {
			int order_id = Integer.valueOf(fields[0]);
			int district_id = Integer.valueOf(fields[1]);
			int warehouse_id = Integer.valueOf(fields[2]);
			if (!_orderTree.containsKey(warehouse_id)) {
				_orderTree.put(warehouse_id,
						new HashMap<Integer, Map<Integer, OrderState>>());
			}
			if (!_orderTree.get(warehouse_id).containsKey(district_id)) {
				_orderTree.get(warehouse_id).put(district_id,
						new HashMap<Integer, OrderState>());
			}
			if (!_orderTree.get(warehouse_id).get(district_id)
					.containsKey(order_id)) {
				_orderTree.get(warehouse_id).get(district_id)
						.put(order_id, new OrderState());
			}
			_orderTree.get(warehouse_id).get(district_id).get(order_id)._prices
					.add(Double.valueOf(fields[8]));

		} else if (streamname.equals("customer")) {
			int customer_id = Integer.valueOf(fields[0]);
			int district_id = Integer.valueOf(fields[1]);
			int warehouse_id = Integer.valueOf(fields[2]);
			if (!_customerTree.containsKey(warehouse_id)) {
				_customerTree.put(warehouse_id,
						new HashMap<Integer, Map<Integer, String>>());
			}
			if (!_customerTree.get(warehouse_id).containsKey(district_id)) {
				_customerTree.get(warehouse_id).put(district_id,
						new HashMap<Integer, String>());
			}
			_customerTree.get(warehouse_id).get(district_id)
					.put(customer_id, fields[9]);
		} else if (streamname.equals("DELIVERY")) {

		} else if (streamname.equals("NEW_ORDER")) {

		} else if (streamname.equals("ORDER_STATUS")) {

		} else if (streamname.equals("PAYMENT")) {

		} else if (streamname.equals("STOCK_LEVEL")) {

		}

		if (streamname == "DELIVERY" || streamname == "NEW_ORDER"
				|| streamname == "ORDER_STATUS" || streamname == "PAYMENT"
				|| streamname == "STOCK_LEVEL") {
			if (System.currentTimeMillis() - _beginTime < 2000) {
				return;
			}
			for (Integer warehouse : _orderTree.keySet()) {
				for (Integer district : _orderTree.get(warehouse).keySet()) {
					for (Integer order : _orderTree.get(warehouse)
							.get(district).keySet()) {
						OrderState tmporder = _orderTree.get(warehouse)
								.get(district).get(order);
						if (tmporder._is_new == true) {
							double ol_amount = 0;
							for (double tmpamount : tmporder._prices) {
								ol_amount += tmpamount;
							}
							int customer_id = tmporder._customer_id;
							String province = _customerTree.get(warehouse)
									.get(district).get(customer_id);
							String outputString = ol_amount + "," + customer_id
									+ "," + province;
						}

					}
				}
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
		// warehouse_id -> district_id -> order_id -> <customer_id, list<price>>
		_orderTree = new HashMap<Integer, Map<Integer, Map<Integer, OrderState>>>();

		// warehouse -> district -> customer_id -> customer_province
		_customerTree = new HashMap<Integer, Map<Integer, Map<Integer, String>>>();
		_beginTime = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
