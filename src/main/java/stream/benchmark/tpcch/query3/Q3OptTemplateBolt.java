package stream.benchmark.tpcch.query3;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.toolkits.MemoryReport;
import stream.benchmark.tpcch.query.TableState.ItemState;
import stream.benchmark.tpcch.query.TableState.OrderLineState;
import stream.benchmark.tpcch.query.TableState.OrderState;
import stream.benchmark.tpcch.query.TableState.DistrictState;
import stream.benchmark.tpcch.query.TableState.CustomerState;
import stream.benchmark.tpcch.spout.BenchmarkConstant;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Q3OptTemplateBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;

	private Map<Integer, ItemState> _itemsIndex;
	private Map<Integer, Map<Integer, DistrictState>> _districtsIndex;
	private Map<Integer, Map<Integer, Map<Integer, CustomerState>>> _customersIndex;
	private Map<Integer, Map<Integer, Map<Integer, OrderState>>> _ordersIndex;
	private Map<Integer, Map<Integer, List<Integer>>> _newordersIndex;
	private Map<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>> _orderlinesIndex;

	private boolean _isFirstQuery = true;
	private long _beginTime;

	private int _numEventCount = 0;
	private int _currentOrderCount = 0;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();

		// item
		if (streamname == "item") {
			int item_id = Integer.valueOf(fields[0]);
			ItemState item = new ItemState(item_id, Integer.valueOf(fields[1]),
					fields[2], Double.valueOf(fields[3]), fields[4]);
			_itemsIndex.put(item_id, item);
		}

		// district
		else if (streamname == "district") {
			int district_id = Integer.valueOf(fields[0]);
			int warehouse_id = Integer.valueOf(fields[1]);
			DistrictState district = new DistrictState(district_id,
					warehouse_id, fields[2], fields[3], fields[4], fields[5],
					fields[6], fields[7], Double.valueOf(fields[8]),
					Double.valueOf(fields[9]), Integer.valueOf(fields[10]));
			if (!_districtsIndex.containsKey(warehouse_id)) {
				_districtsIndex.put(warehouse_id,
						new HashMap<Integer, DistrictState>());
			}
			_districtsIndex.get(warehouse_id).put(district_id, district);
		}

		// customer
		else if (streamname == "customer") {
			int customer_id = Integer.valueOf(fields[0]);
			int district_id = Integer.valueOf(fields[1]);
			int warehouse_id = Integer.valueOf(fields[2]);
			CustomerState customer = new CustomerState(customer_id,
					district_id, warehouse_id, fields[3], fields[4], fields[5],
					fields[6], fields[7], fields[8], fields[9], fields[10],
					fields[11], Long.valueOf(fields[12]), fields[13],
					Double.valueOf(fields[14]), Double.valueOf(fields[15]),
					Double.valueOf(fields[16]), Double.valueOf(fields[17]),
					Integer.valueOf(fields[18]), Integer.valueOf(fields[19]),
					fields[20]);
			if (!_customersIndex.containsKey(warehouse_id)) {
				_customersIndex.put(warehouse_id,
						new HashMap<Integer, Map<Integer, CustomerState>>());
			}
			if (!_customersIndex.get(warehouse_id).containsKey(district_id)) {
				_customersIndex.get(warehouse_id).put(district_id,
						new HashMap<Integer, CustomerState>());
			}
			_customersIndex.get(warehouse_id).get(district_id)
					.put(customer_id, customer);
		}

		// order
		else if (streamname == "order") {
			int order_id = Integer.valueOf(fields[0]);
			int customer_id = Integer.valueOf(fields[1]);
			int district_id = Integer.valueOf(fields[2]);
			int warehouse_id = Integer.valueOf(fields[3]);
			OrderState order = new OrderState(order_id, customer_id,
					district_id, warehouse_id, Long.valueOf(fields[4]),
					Integer.valueOf(fields[5]), Integer.valueOf(fields[6]),
					Boolean.valueOf(fields[7]));
			if (!_ordersIndex.containsKey(warehouse_id)) {
				_ordersIndex.put(warehouse_id,
						new HashMap<Integer, Map<Integer, OrderState>>());
			}
			if (!_ordersIndex.get(warehouse_id).containsKey(district_id)) {
				_ordersIndex.get(warehouse_id).put(district_id,
						new HashMap<Integer, OrderState>());
			}
			_ordersIndex.get(warehouse_id).get(district_id)
					.put(order_id, order);
		}

		// new order
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
			_newordersIndex.get(warehouse_id).get(district_id).add(order_id);
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
			if (!_orderlinesIndex.get(warehouse_id).containsKey(district_id)) {
				_orderlinesIndex.get(warehouse_id).put(district_id,
						new HashMap<Integer, List<OrderLineState>>());
			}
			if (!_orderlinesIndex.get(warehouse_id).get(district_id)
					.containsKey(order_id)) {
				_orderlinesIndex.get(warehouse_id).get(district_id)
						.put(order_id, new LinkedList<OrderLineState>());
			}
			_orderlinesIndex.get(warehouse_id).get(district_id).get(order_id)
					.add(orderline);
		}

		else if (streamname == "DELIVERY") {
			int w_id = Integer.valueOf(fields[0]);
			long ol_delivery_d = Long.valueOf(fields[2]);
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
				for (OrderLineState tmp : _orderlinesIndex.get(w_id).get(d_id)
						.get(no_o_id)) {
					tmp._ol_delivery_d = ol_delivery_d;
				}
			}
			// event count
			_numEventCount += 1;
		}

		else if (streamname == "NEW_ORDER") {
			int w_id = Integer.valueOf(fields[0]);
			int d_id = Integer.valueOf(fields[1]);
			int c_id = Integer.valueOf(fields[2]);
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

			int ol_cnt = i_ids.size();
			int o_carrier_id = BenchmarkConstant.NULL_CARRIER_ID;
			// createOrder : d_next_o_id, d_id, w_id, c_id, o_entry_d,
			// o_carrier_id, ol_cnt, all_local
			OrderState orderState = new OrderState(d_next_o_id, c_id, d_id,
					w_id, o_entry_d, o_carrier_id, ol_cnt, true);
			_ordersIndex.get(w_id).get(d_id).put(d_next_o_id, orderState);

			for (int i = 0; i < i_ids.size(); ++i) {
				int ol_number = i + 1;
				int ol_supply_w_id = i_w_ids.get(i);
				int ol_i_id = i_ids.get(i);
				int ol_quantity = i_qtys.get(i);
				double ol_amount = ol_quantity
						* _itemsIndex.get(i_ids.get(i))._price;
				// create new order line
				OrderLineState olState = new OrderLineState(d_next_o_id, d_id,
						w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d,
						ol_quantity, ol_amount, "dist_info");
				if (!_orderlinesIndex.get(w_id).get(d_id)
						.containsKey(d_next_o_id)) {
					_orderlinesIndex.get(w_id).get(d_id)
							.put(d_next_o_id, new LinkedList<OrderLineState>());
				}
				_orderlinesIndex.get(w_id).get(d_id).get(d_next_o_id)
						.add(olState);
			}

			if (!_newordersIndex.containsKey(w_id)) {
				_newordersIndex
						.put(w_id, new HashMap<Integer, List<Integer>>());
			}
			if (!_newordersIndex.get(w_id).containsKey(d_id)) {
				_newordersIndex.get(w_id).put(d_id, new LinkedList<Integer>());
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

			} else if (_numEventCount % 200000 == 0) {
				_numEventCount = 0;
				System.out
						.println("################################################");
				System.out.println("elapsed consume time = "
						+ (System.currentTimeMillis() - _beginTime) + "ms");
				// /////////////////////////////////////////////////////////////////
				long startQueryTime = System.currentTimeMillis();
				StringBuilder sb = new StringBuilder();

				for (int warehouse : _newordersIndex.keySet()) {
					for (int district : _newordersIndex.get(warehouse).keySet()) {
						for (int order : _newordersIndex.get(warehouse).get(
								district)) {
							if (_currentOrderCount
									- _ordersIndex.get(warehouse).get(district)
											.get(order)._id < 20000) {
								double ol_amount = 0;
								for (OrderLineState tmpol : _orderlinesIndex
										.get(warehouse).get(district)
										.get(order)) {
									ol_amount += tmpol._ol_amount;
								}
								int customer_id = _ordersIndex.get(warehouse)
										.get(district).get(order)._c_id;
								String province = _customersIndex
										.get(warehouse).get(district)
										.get(customer_id)._state;
								sb.append(warehouse);
								sb.append(", ");
								sb.append(district);
								sb.append(", ");
								sb.append(customer_id);
								sb.append(", ");
								sb.append(province);
								sb.append(", ");
								sb.append(ol_amount);
								_collector.emit(new Values(sb.toString()));
								sb.setLength(0);
							}
						}
					}
				}

				System.out.println("elapsed query time = "
						+ (System.currentTimeMillis() - startQueryTime) + "ms");

				System.out.println("===================================");
				MemoryReport.reportStatus();
				_beginTime = System.currentTimeMillis();
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;

		_districtsIndex = new HashMap<Integer, Map<Integer, DistrictState>>();
		_itemsIndex = new HashMap<Integer, ItemState>();
		_customersIndex = new HashMap<Integer, Map<Integer, Map<Integer, CustomerState>>>();
		_ordersIndex = new HashMap<Integer, Map<Integer, Map<Integer, OrderState>>>();
		_newordersIndex = new HashMap<Integer, Map<Integer, List<Integer>>>();
		_orderlinesIndex = new HashMap<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>>();

		_beginTime = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
