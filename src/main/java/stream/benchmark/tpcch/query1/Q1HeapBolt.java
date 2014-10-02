package stream.benchmark.tpcch.query1;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.toolkits.MemoryReport;
import stream.benchmark.tpcch.query.InnerState.NewOrderItemInfo;
import stream.benchmark.tpcch.query.TableState.ItemState;
import stream.benchmark.tpcch.query.TableState.NewOrderState;
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

public class Q1HeapBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;

	private List<ItemState> _items;
	private Map<Integer, ItemState> _itemsIndex;
	private List<DistrictState> _districts;
	private Map<Integer, Map<Integer, DistrictState>> _districtsIndex;
	private List<NewOrderState> _neworders;
	private Map<Integer, Map<Integer, List<Integer>>> _newordersIndex;
	private List<OrderLineState> _orderlines;
	private Map<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>> _orderlinesIndex;

	private boolean _isFirstQuery = true;
	private long _beginTime;

	private int _numItems = 0;
	private int _numDistricts = 0;
	private int _numNeworders = 0;
	private int _numOrderlines = 0;

	private int _numReadItems = 0;
	private int _numReadDistricts = 0;
	private int _numReadNeworders = 0;
	private int _numReadOrderlines = 0;
	private int _numWriteItems = 0;
	private int _numWriteDistricts = 0;
	private int _numWriteNeworders = 0;
	private int _numWriteOrderlines = 0;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname == "item") {
			int item_id = Integer.valueOf(fields[0]);
			ItemState item = new ItemState(item_id, Integer.valueOf(fields[1]),
					fields[2], Double.valueOf(fields[3]), fields[4]);
			_items.add(item);
			_itemsIndex.put(item_id, item);
			++_numItems;
		} else if (streamname == "district") {
			int district_id = Integer.valueOf(fields[0]);
			int warehouse_id = Integer.valueOf(fields[1]);
			DistrictState district = new DistrictState(district_id,
					warehouse_id, fields[2], fields[3], fields[4], fields[5],
					fields[6], fields[7], Double.valueOf(fields[8]),
					Double.valueOf(fields[9]), Integer.valueOf(fields[10]));
			_districts.add(district);
			if (!_districtsIndex.containsKey(warehouse_id)) {
				_districtsIndex.put(warehouse_id,
						new HashMap<Integer, DistrictState>());
			}
			_districtsIndex.get(warehouse_id).put(district_id, district);
			++_numDistricts;
		} else if (streamname == "neworder") {
			int order_id = Integer.valueOf(fields[0]);
			int district_id = Integer.valueOf(fields[1]);
			int warehouse_id = Integer.valueOf(fields[2]);
			NewOrderState neworder = new NewOrderState(order_id, district_id,
					warehouse_id);
			_neworders.add(neworder);
			if (!_newordersIndex.containsKey(warehouse_id)) {
				_newordersIndex.put(warehouse_id,
						new HashMap<Integer, List<Integer>>());
			}
			if (!_newordersIndex.get(warehouse_id).containsKey(district_id)) {
				_newordersIndex.get(warehouse_id).put(district_id,
						new LinkedList<Integer>());
			}
			_newordersIndex.get(warehouse_id).get(district_id).add(order_id);
			++_numNeworders;
		} else if (streamname == "orderline") {
			int order_id = Integer.valueOf(fields[0]);
			int district_id = Integer.valueOf(fields[1]);
			int warehouse_id = Integer.valueOf(fields[2]);
			OrderLineState orderline = new OrderLineState(order_id,
					district_id, warehouse_id, Integer.valueOf(fields[3]),
					Integer.valueOf(fields[4]), Integer.valueOf(fields[5]),
					Long.valueOf(fields[6]), Integer.valueOf(fields[7]),
					Double.valueOf(fields[8]), fields[9]);
			_orderlines.add(orderline);
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
			++_numOrderlines;
		} else if (streamname == "DELIVERY") {
			int w_id = Integer.valueOf(fields[0]);
			// long ol_delivery_d = Long.valueOf(fields[2]);
			long ol_delivery_d = System.currentTimeMillis();
			// for each district, deliver the first new_order
			for (int d_id = 1; d_id < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE + 1; ++d_id) {
				// randomly pick a new order
				if (_newordersIndex.get(w_id).get(d_id).size() == 0) {
					continue;
				}
				// getNewOrder: no_d_id, no_w_id
				int no_o_id = _newordersIndex.get(w_id).get(d_id).get(0);
				++_numReadNeworders;
				// deleteNewOrder : d_id, w_id, no_o_id
				_newordersIndex.get(w_id).get(d_id).remove(0);
				++_numWriteNeworders;
				Iterator<NewOrderState> iter = _neworders.iterator();
				while (iter.hasNext()) {
					NewOrderState tmp = iter.next();
					if (tmp._o_id == no_o_id) {
						iter.remove();
						break;
					}
				}
				// updateOrderLine : ol_delivery_d, no_o_id, d_id, w_id
				for (OrderLineState tmp : _orderlinesIndex.get(w_id).get(d_id)
						.get(no_o_id)) {
					tmp._ol_delivery_d = ol_delivery_d;
					++_numWriteNeworders;
				}
			}
		} else if (streamname == "NEW_ORDER") {
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

			List<NewOrderItemInfo> item_infos = new LinkedList<NewOrderItemInfo>();
			for (int i = 0; i < i_ids.size(); ++i) {
				ItemState tmpItem = _itemsIndex.get(i_ids.get(i));
				++_numReadItems;
				item_infos.add(new NewOrderItemInfo(tmpItem._price,
						tmpItem._name, tmpItem._data));
			}
			// getDistrict : d_id, w_id
			DistrictState tmpDistrict = _districtsIndex.get(w_id).get(d_id);
			int d_next_o_id = tmpDistrict._next_o_id;
			++_numReadDistricts;
			_districtsIndex.get(w_id).get(d_id)._next_o_id = d_next_o_id + 1;
			++_numWriteDistricts;

			// createNewOrder : d_next_o_id, d_id, w_id
			NewOrderState neworderState = new NewOrderState(d_next_o_id, d_id,
					w_id);
			_neworders.add(neworderState);
			_newordersIndex.get(w_id).get(d_id).add(d_next_o_id);
			++_numWriteNeworders;
			++_numNeworders;
			for (int i = 0; i < i_ids.size(); ++i) {
				int ol_number = i + 1;
				int ol_supply_w_id = i_w_ids.get(i);
				int ol_i_id = i_ids.get(i);
				int ol_quantity = i_qtys.get(i);
				NewOrderItemInfo tmpItemInfo = item_infos.get(i);
				double ol_amount = ol_quantity * tmpItemInfo._i_price;
				// create new order line
				OrderLineState olState = new OrderLineState(d_next_o_id, d_id,
						w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d,
						ol_quantity, ol_amount, "dist_info");
				_orderlines.add(olState);
				if (!_orderlinesIndex.get(w_id).get(d_id)
						.containsKey(d_next_o_id)) {
					_orderlinesIndex.get(w_id).get(d_id)
							.put(d_next_o_id, new LinkedList<OrderLineState>());
				}
				_orderlinesIndex.get(w_id).get(d_id).get(d_next_o_id)
						.add(olState);
				++_numWriteOrderlines;
				++_numOrderlines;
			}
		}

		if (streamname == "DELIVERY" || streamname == "NEW_ORDER"
				|| streamname == "ORDER_STATUS" || streamname == "PAYMENT"
				|| streamname == "STOCK_LEVEL") {
			if (_isFirstQuery) {
				long elapsedTime = System.currentTimeMillis() - _beginTime;
				System.out.println("load database elapsed time = "
						+ elapsedTime + "ms");
				_isFirstQuery = false;
				_beginTime = System.currentTimeMillis();

			} else if (System.currentTimeMillis() - _beginTime >= 2000) {

				// /////////////////////////////////////////////////////////////////
				long startQueryTime = System.currentTimeMillis();
				StringBuilder sb = new StringBuilder();
				for (Integer warehouse : _orderlinesIndex.keySet()) {
					for (Integer district : _orderlinesIndex.get(warehouse)
							.keySet()) {
						int quantity_sum = 0;
						int amount_sum = 0;
						int orderline_num = 0;
						for (int customer : _orderlinesIndex.get(warehouse)
								.get(district).keySet()) {
							for (OrderLineState state : _orderlinesIndex
									.get(warehouse).get(district).get(customer)) {
								if (System.currentTimeMillis()
										- state._ol_delivery_d < 3000) {
									quantity_sum += state._ol_quantity;
									amount_sum += state._ol_amount;
									++orderline_num;
									++_numReadOrderlines;
								}
							}

							// Iterator<OrderLineState> iter =
							// _orderlinesIndex.get(warehouse).get(district).get(customer).iterator();
							// while (iter.hasNext()) {
							// OrderLineState state = iter.next();
							// if (System.currentTimeMillis()
							// - state._ol_delivery_d < 10000) {
							// quantity_sum += state._ol_quantity;
							// amount_sum += state._ol_amount;
							// ++orderline_num;
							// }
							// else{
							// iter.remove();
							// break;
							// }
							// }

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
						sb.setLength(0);
					}
				}

				MemoryReport.reportStatus();

				System.out.println("=============ORIGIN===============");
				System.out.println("item num=" + _numItems);
				System.out.println("district num=" + _numDistricts);
				System.out.println("neworder num=" + _numNeworders);
				System.out.println("orderline num=" + _numOrderlines);

				System.out.println("*************CURRENT**************");

				System.out.println("item size=" + _items.size());
				System.out.println("district size=" + _districts.size());
				System.out.println("neworder size=" + _neworders.size());
				System.out.println("orderline size=" + _orderlines.size());
				System.out.println("===================================");

				System.out.println("*************READ**************");
				System.out.println("read item size=" + _numReadItems);
				System.out.println("read district size=" + _numReadDistricts);
				System.out.println("read neworder size=" + _numReadNeworders);
				System.out.println("read orderline size=" + _numReadOrderlines);
				System.out.println("===================================");

				System.out.println("*************WRITE**************");
				System.out.println("write item size=" + _numWriteItems);
				System.out.println("write district size=" + _numWriteDistricts);
				System.out.println("write neworder size=" + _numWriteNeworders);
				System.out.println("write orderline size="
						+ _numWriteOrderlines);
				System.out.println("===================================");

				System.out.println("elapsed query time = "
						+ (System.currentTimeMillis() - startQueryTime) + "ms");

				_numItems = 0;
				_numDistricts = 0;
				_numNeworders = 0;
				_numOrderlines = 0;

				_numReadItems = 0;
				_numReadDistricts = 0;
				_numReadNeworders = 0;
				_numReadOrderlines = 0;
				_numWriteItems = 0;
				_numWriteDistricts = 0;
				_numWriteNeworders = 0;
				_numWriteOrderlines = 0;

				_beginTime = System.currentTimeMillis();
			}

		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;

		_items = new LinkedList<ItemState>();
		_itemsIndex = new HashMap<Integer, ItemState>();

		_districts = new LinkedList<DistrictState>();
		_districtsIndex = new HashMap<Integer, Map<Integer, DistrictState>>();
		_neworders = new LinkedList<NewOrderState>();
		_newordersIndex = new HashMap<Integer, Map<Integer, List<Integer>>>();
		_orderlines = new LinkedList<OrderLineState>();
		_orderlinesIndex = new HashMap<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>>();

		_beginTime = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
