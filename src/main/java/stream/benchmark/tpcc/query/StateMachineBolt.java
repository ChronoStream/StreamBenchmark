package stream.benchmark.tpcc.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.tpcc.query.FetchResult.NewOrderItemInfo;
import stream.benchmark.tpcc.query.FetchResult.NewOrderItemData;
import stream.benchmark.tpcc.query.TableState.HistoryState;
import stream.benchmark.tpcc.query.TableState.ItemState;
import stream.benchmark.tpcc.query.TableState.NewOrderState;
import stream.benchmark.tpcc.query.TableState.OrderLineState;
import stream.benchmark.tpcc.query.TableState.OrderState;
import stream.benchmark.tpcc.query.TableState.StockState;
import stream.benchmark.tpcc.query.TableState.WarehouseState;
import stream.benchmark.tpcc.query.TableState.DistrictState;
import stream.benchmark.tpcc.query.TableState.CustomerState;
import stream.benchmark.tpcc.spout.BenchmarkConstant;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StateMachineBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;

	private List<ItemState> _items;
	private Map<Integer, ItemState> _itemsIndex;

	private List<WarehouseState> _warehouses;
	private Map<Integer, WarehouseState> _warehousesIndex;
	private List<DistrictState> _districts;
	private Map<Integer, Map<Integer, DistrictState>> _districtsIndex;
	private List<CustomerState> _customers;
	private Map<Integer, Map<Integer, Map<Integer, CustomerState>>> _customersIndex;
	private List<OrderState> _orders;
	private Map<Integer, Map<Integer, Map<Integer, OrderState>>> _ordersIndex;
	private List<NewOrderState> _neworders;
	private Map<Integer, Map<Integer, List<Integer>>> _newordersIndex;
	private List<OrderLineState> _orderlines;
	private Map<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>> _orderlinesIndex;
	private List<HistoryState> _histories;
	private Map<Integer, Map<Integer, Map<Integer, HistoryState>>> _historiesIndex;
	private List<StockState> _stocks;
	private Map<Integer, Map<Integer, StockState>> _stocksIndex;

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
		} else if (streamname == "warehouse") {
			int warehouse_id = Integer.valueOf(fields[0]);
			WarehouseState warehouse = new WarehouseState(warehouse_id,
					fields[1], fields[2], fields[3], fields[4], fields[5],
					fields[6], Double.valueOf(fields[7]),
					Double.valueOf(fields[8]));
			_warehouses.add(warehouse);
			_warehousesIndex.put(Integer.valueOf(fields[0]), warehouse);
			// ===========
			System.out.println("warehouse_id=" + warehouse_id);
			// ===========
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
			// ===========
			System.out.println("warehouse_id=" + warehouse_id
					+ ", district_id=" + district_id);
			// ===========
		} else if (streamname == "customer") {
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
			_customers.add(customer);
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
		} else if (streamname == "stock") {
			int item_id = Integer.valueOf(fields[0]);
			int warehouse_id = Integer.valueOf(fields[1]);
			int quantity = Integer.valueOf(fields[2]);
			List<String> dists = new LinkedList<String>();
			int tmpId = 3;
			for (; tmpId < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE + 3; ++tmpId) {
				dists.add(fields[tmpId]);
			}
			System.out.println("dist length=" + dists.size());
			int ytd = Integer.valueOf(fields[tmpId++]);
			int order_cnt = Integer.valueOf(fields[tmpId++]);
			int remote_cnt = Integer.valueOf(fields[tmpId++]);
			String data = fields[tmpId++];
			StockState stock = new StockState(item_id, warehouse_id, quantity,
					dists, ytd, order_cnt, remote_cnt, data);
			_stocks.add(stock);
			if (!_stocksIndex.containsKey(item_id)) {
				_stocksIndex.put(item_id, new HashMap<Integer, StockState>());
			}
			if (!_stocksIndex.get(item_id).containsKey(warehouse_id)) {
				_stocksIndex.get(item_id).put(warehouse_id, stock);
			}
		} else if (streamname == "order") {
			int order_id = Integer.valueOf(fields[0]);
			int customer_id = Integer.valueOf(fields[1]);
			int district_id = Integer.valueOf(fields[2]);
			int warehouse_id = Integer.valueOf(fields[3]);
			OrderState order = new OrderState(order_id, customer_id,
					district_id, warehouse_id, Long.valueOf(fields[4]),
					Integer.valueOf(fields[5]), Double.valueOf(fields[6]),
					Boolean.valueOf(fields[7]));
			_orders.add(order);
			System.out.println("order=" + order_id);
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
		} else if (streamname == "history") {
			int h_c_id = Integer.valueOf(fields[0]);
			int h_c_d_id = Integer.valueOf(fields[1]);
			int h_c_w_id = Integer.valueOf(fields[2]);
			HistoryState history = new HistoryState(h_c_id, h_c_d_id, h_c_w_id,
					Integer.valueOf(fields[3]), Integer.valueOf(fields[4]),
					Long.valueOf(fields[5]), Double.valueOf(fields[6]),
					fields[7]);
			_histories.add(history);
			if (!_historiesIndex.containsKey(h_c_w_id)) {
				_historiesIndex.put(h_c_w_id,
						new HashMap<Integer, Map<Integer, HistoryState>>());
			}
			if (!_historiesIndex.get(h_c_w_id).containsKey(h_c_d_id)) {
				_historiesIndex.get(h_c_w_id).put(h_c_d_id,
						new HashMap<Integer, HistoryState>());
			}
			_historiesIndex.get(h_c_w_id).get(h_c_d_id).put(h_c_id, history);
		} else if (streamname == "DELIVERY") {
			int w_id = Integer.valueOf(fields[0]);
			System.out.println("w_id=" + w_id);
			int o_carrier_id = Integer.valueOf(fields[1]);
			System.out.println("o_carrier_id=" + o_carrier_id);
			long ol_delivery_d = Long.valueOf(fields[2]);
			System.out.println("ol_delivery_d=" + ol_delivery_d);
			// for each district, deliver the first new_order
			for (int d_id = 1; d_id < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE + 1; ++d_id) {
				// randomly pick a new order
				if (_newordersIndex.get(w_id).get(d_id).size() == 0) {
					continue;
				}
				// getNewOrder: no_d_id, no_w_id
				int no_o_id = _newordersIndex.get(w_id).get(d_id).get(0);
				System.out.println("no_o_id=" + no_o_id);
				// getCId: no_o_id, d_id, w_id
				System.out
						.println("check contain="
								+ _ordersIndex.get(w_id).get(d_id)
										.containsKey(no_o_id));
				int c_id = _ordersIndex.get(w_id).get(d_id).get(no_o_id)._c_id;
				// sumOLAmount: no_o_id, d_id, w_id
				int sum = 0;
				List<OrderLineState> orderlineList = _orderlinesIndex.get(w_id)
						.get(d_id).get(no_o_id);
				for (OrderLineState state : orderlineList) {
					sum += state._ol_amount;
				}
				// deleteNewOrder : d_id, w_id, no_o_id
				_newordersIndex.get(w_id).get(d_id).remove(0);
				Iterator<NewOrderState> iter = _neworders.iterator();
				while (iter.hasNext()) {
					NewOrderState tmp = iter.next();
					if (tmp._o_id == no_o_id) {
						iter.remove();
					}
				}
				// updateOrders : o_carrier_id, no_o_id, d_id, w_id
				_ordersIndex.get(w_id).get(d_id).get(no_o_id)._carrier_id = o_carrier_id;

				// updateOrderLine : ol_delivery_d, no_o_id, d_id, w_id
				List<OrderLineState> tmpList = _orderlinesIndex.get(w_id)
						.get(d_id).get(no_o_id);
				for (OrderLineState tmp : tmpList) {
					tmp._ol_delivery_d = ol_delivery_d;
				}
				// updateCustomer : ol_total, c_id, d_id, w_id
				_customersIndex.get(w_id).get(d_id).get(c_id)._balance += sum;
			}
		} else if (streamname == "NEW_ORDER") {
			System.out.println("new_order=" + tuple);
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

			boolean all_local = true;
			List<NewOrderItemInfo> item_infos = new LinkedList<NewOrderItemInfo>();
			for (int i = 0; i < i_ids.size(); ++i) {
				all_local = (all_local && (w_id == i_w_ids.get(i)));
				ItemState tmpItem = _itemsIndex.get(i_ids.get(i));
				item_infos.add(new NewOrderItemInfo(tmpItem._price,
						tmpItem._name, tmpItem._data));
			}
			// getWarehouseTaxRate
			double w_tax = _warehousesIndex.get(w_id)._tax;

			// getDistrict : d_id, w_id
			DistrictState tmpDistrict = _districtsIndex.get(w_id).get(d_id);
			double d_tax = tmpDistrict._tax;
			int d_next_o_id = tmpDistrict._next_o_id;

			// getCustomer : w_id, d_id, c_id
			CustomerState tmpCustomer = _customersIndex.get(w_id).get(d_id)
					.get(c_id);
			double c_discount = tmpCustomer._discount;

			int ol_cnt = i_ids.size();
			int o_carrier_id = BenchmarkConstant.NULL_CARRIER_ID;

			// incrementNextOrderId : d_next_o_id + 1, d_id, w_id
			_districtsIndex.get(w_id).get(d_id)._next_o_id = d_next_o_id + 1;

			// createOrder : d_next_o_id, d_id, w_id, c_id, o_entry_d,
			// o_carrier_id, ol_cnt, all_local
			_orders.add(new OrderState(d_next_o_id, d_id, w_id, c_id,
					o_entry_d, o_carrier_id, ol_cnt, all_local));

			// createNewOrder : d_next_o_id, d_id, w_id
			_neworders.add(new NewOrderState(d_next_o_id, d_id, w_id));

			List<NewOrderItemData> item_datas = new LinkedList<NewOrderItemData>();
			for (int i = 0; i < i_ids.size(); ++i) {
				int ol_number = i + 1;
				int ol_supply_w_id = i_w_ids.get(i);
				int ol_i_id = i_ids.get(i);
				int ol_quantity = i_qtys.get(i);
				NewOrderItemInfo tmpItemInfo = item_infos.get(i);

				// getStockInfo :

			}

		} else if (streamname == "ORDER_STATUS") {

		} else if (streamname == "PAYMENT") {

		} else if (streamname == "STOCK_LEVEL") {
			// int w_id = Integer.valueOf(fields[0]);
			// int d_id = Integer.valueOf(fields[1]);
			// int threshold = Integer.valueOf(fields[2]);
			// get order id
			// get warehouse_id, district_id, order_id,
		} else {

		}
		_collector.emit(new Values(streamname));
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;

		_items = new LinkedList<ItemState>();
		_itemsIndex = new HashMap<Integer, ItemState>();

		_warehouses = new LinkedList<WarehouseState>();
		_warehousesIndex = new HashMap<Integer, WarehouseState>();
		_districts = new LinkedList<DistrictState>();
		_districtsIndex = new HashMap<Integer, Map<Integer, DistrictState>>();
		_customers = new LinkedList<CustomerState>();
		_customersIndex = new HashMap<Integer, Map<Integer, Map<Integer, CustomerState>>>();
		_orders = new LinkedList<OrderState>();
		_ordersIndex = new HashMap<Integer, Map<Integer, Map<Integer, OrderState>>>();
		_neworders = new LinkedList<NewOrderState>();
		_newordersIndex = new HashMap<Integer, Map<Integer, List<Integer>>>();
		_orderlines = new LinkedList<OrderLineState>();
		_orderlinesIndex = new HashMap<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>>();
		_histories = new LinkedList<HistoryState>();
		_historiesIndex = new HashMap<Integer, Map<Integer, Map<Integer, HistoryState>>>();
		_stocks = new LinkedList<StockState>();
		_stocksIndex = new HashMap<Integer, Map<Integer, StockState>>();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
