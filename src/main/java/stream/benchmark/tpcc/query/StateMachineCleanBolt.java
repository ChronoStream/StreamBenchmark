package stream.benchmark.tpcc.query;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.toolkits.MemoryReport;
import stream.benchmark.tpcc.query.TableState.HistoryState;
import stream.benchmark.tpcc.query.TableState.ItemState;
import stream.benchmark.tpcc.query.TableState.OrderLineState;
import stream.benchmark.tpcc.query.TableState.OrderState;
import stream.benchmark.tpcc.query.TableState.StockState;
import stream.benchmark.tpcc.query.TableState.WarehouseState;
import stream.benchmark.tpcc.query.TableState.DistrictState;
import stream.benchmark.tpcc.query.TableState.CustomerState;
import stream.benchmark.tpcc.spout.BenchmarkConstant;
import stream.benchmark.tpcc.spout.BenchmarkRandom;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StateMachineCleanBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;

	private Map<Integer, ItemState> _items;
	private Map<Integer, WarehouseState> _warehouses;
	private Map<Integer, Map<Integer, DistrictState>> _districts;
	private Map<Integer, Map<Integer, Map<Integer, CustomerState>>> _customers;
	private Map<Integer, Map<Integer, Map<Integer, OrderState>>> _orders;
	private Map<Integer, Map<Integer, List<Integer>>> _neworders;
	private Map<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>> _orderlines;
	private List<HistoryState> _histories;
	private Map<Integer, Map<Integer, StockState>> _stocks;

	private boolean _isFirstQuery = true;
	private long _beginTime;

	private int _numItems = 0;
	private int _numWarehouses = 0;
	private int _numDistricts = 0;
	private int _numCustomers = 0;
	private int _numStocks = 0;
	private int _numOrders = 0;
	private int _numNeworders = 0;
	private int _numOrderlines = 0;
	private int _numHistories = 0;

	private int _numEventCount = 0;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname == "item") {
			int item_id = Integer.valueOf(fields[0]);
			ItemState item = new ItemState(item_id, Integer.valueOf(fields[1]),
					fields[2], Double.valueOf(fields[3]), fields[4]);
			_items.put(item_id, item);
			++_numItems;
		} else if (streamname == "warehouse") {
			int warehouse_id = Integer.valueOf(fields[0]);
			WarehouseState warehouse = new WarehouseState(warehouse_id,
					fields[1], fields[2], fields[3], fields[4], fields[5],
					fields[6], Double.valueOf(fields[7]),
					Double.valueOf(fields[8]));
			_warehouses.put(Integer.valueOf(fields[0]), warehouse);
			++_numWarehouses;
		} else if (streamname == "district") {
			int district_id = Integer.valueOf(fields[0]);
			int warehouse_id = Integer.valueOf(fields[1]);
			DistrictState district = new DistrictState(district_id,
					warehouse_id, fields[2], fields[3], fields[4], fields[5],
					fields[6], fields[7], Double.valueOf(fields[8]),
					Double.valueOf(fields[9]), Integer.valueOf(fields[10]));
			if (!_districts.containsKey(warehouse_id)) {
				_districts.put(warehouse_id,
						new HashMap<Integer, DistrictState>());
			}
			_districts.get(warehouse_id).put(district_id, district);
			++_numDistricts;
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
			if (!_customers.containsKey(warehouse_id)) {
				_customers.put(warehouse_id,
						new HashMap<Integer, Map<Integer, CustomerState>>());
			}
			if (!_customers.get(warehouse_id).containsKey(district_id)) {
				_customers.get(warehouse_id).put(district_id,
						new HashMap<Integer, CustomerState>());
			}
			_customers.get(warehouse_id).get(district_id)
					.put(customer_id, customer);
			++_numCustomers;
		} else if (streamname == "stock") {
			int item_id = Integer.valueOf(fields[0]);
			int warehouse_id = Integer.valueOf(fields[1]);
			int quantity = Integer.valueOf(fields[2]);
			List<String> dists = new LinkedList<String>();
			int tmpId = 3;
			for (; tmpId < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE + 3; ++tmpId) {
				dists.add(fields[tmpId]);
			}
			int ytd = Integer.valueOf(fields[tmpId++]);
			int order_cnt = Integer.valueOf(fields[tmpId++]);
			int remote_cnt = Integer.valueOf(fields[tmpId++]);
			String data = fields[tmpId++];
			StockState stock = new StockState(item_id, warehouse_id, quantity,
					dists, ytd, order_cnt, remote_cnt, data);
			if (!_stocks.containsKey(item_id)) {
				_stocks.put(item_id, new HashMap<Integer, StockState>());
			}
			if (!_stocks.get(item_id).containsKey(warehouse_id)) {
				_stocks.get(item_id).put(warehouse_id, stock);
			}
			++_numStocks;
		} else if (streamname == "order") {
			int order_id = Integer.valueOf(fields[0]);
			int customer_id = Integer.valueOf(fields[1]);
			int district_id = Integer.valueOf(fields[2]);
			int warehouse_id = Integer.valueOf(fields[3]);
			OrderState order = new OrderState(order_id, customer_id,
					district_id, warehouse_id, Long.valueOf(fields[4]),
					Integer.valueOf(fields[5]), Integer.valueOf(fields[6]),
					Boolean.valueOf(fields[7]));
			if (!_orders.containsKey(warehouse_id)) {
				_orders.put(warehouse_id,
						new HashMap<Integer, Map<Integer, OrderState>>());
			}
			if (!_orders.get(warehouse_id).containsKey(district_id)) {
				_orders.get(warehouse_id).put(district_id,
						new HashMap<Integer, OrderState>());
			}
			_orders.get(warehouse_id).get(district_id).put(order_id, order);
			++_numOrders;
		} else if (streamname == "neworder") {
			int order_id = Integer.valueOf(fields[0]);
			int district_id = Integer.valueOf(fields[1]);
			int warehouse_id = Integer.valueOf(fields[2]);
			if (!_neworders.containsKey(warehouse_id)) {
				_neworders.put(warehouse_id,
						new HashMap<Integer, List<Integer>>());
			}
			if (!_neworders.get(warehouse_id).containsKey(district_id)) {
				_neworders.get(warehouse_id).put(district_id,
						new LinkedList<Integer>());
			}
			_neworders.get(warehouse_id).get(district_id).add(order_id);
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
			if (!_orderlines.containsKey(warehouse_id)) {
				_orderlines
						.put(warehouse_id,
								new HashMap<Integer, Map<Integer, List<OrderLineState>>>());
			}
			if (!_orderlines.get(warehouse_id).containsKey(district_id)) {
				_orderlines.get(warehouse_id).put(district_id,
						new HashMap<Integer, List<OrderLineState>>());
			}
			if (!_orderlines.get(warehouse_id).get(district_id)
					.containsKey(order_id)) {
				_orderlines.get(warehouse_id).get(district_id)
						.put(order_id, new LinkedList<OrderLineState>());
			}
			_orderlines.get(warehouse_id).get(district_id).get(order_id)
					.add(orderline);
			++_numOrderlines;
		} else if (streamname == "history") {
			int h_c_id = Integer.valueOf(fields[0]);
			int h_c_d_id = Integer.valueOf(fields[1]);
			int h_c_w_id = Integer.valueOf(fields[2]);
			HistoryState history = new HistoryState(h_c_id, h_c_d_id, h_c_w_id,
					Integer.valueOf(fields[3]), Integer.valueOf(fields[4]),
					Long.valueOf(fields[5]), Double.valueOf(fields[6]),
					fields[7]);
			_histories.add(history);
			++_numHistories;
		}
		// DELIVERY
		else if (streamname.equals("DELIVERY")) {
			int w_id = Integer.valueOf(fields[0]);
			int o_carrier_id = Integer.valueOf(fields[1]);
			long ol_delivery_d = Long.valueOf(fields[2]);
			// for each district, deliver the first new_order
			for (int d_id = 1; d_id < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE + 1; ++d_id) {
				// getNewOrder: no_d_id, no_w_id
				// remove the oldest one
				// neworders -> input
				if (_neworders.get(w_id).get(d_id).size() == 0) {
					continue;
				}
				int no_o_id = _neworders.get(w_id).get(d_id).remove(0);

				// sumOLAmount: no_o_id, d_id, w_id
				// updateOrderLine : ol_delivery_d, no_o_id, d_id, w_id
				// orderlines -> neworders
				double sum = 0;
				List<OrderLineState> orderlineList = _orderlines.get(w_id)
						.get(d_id).get(no_o_id);
				for (OrderLineState state : orderlineList) {
					sum += state._ol_amount;
					state._ol_delivery_d = ol_delivery_d;
				}

				// updateOrders : o_carrier_id, no_o_id, d_id, w_id
				// getCId: no_o_id, d_id, w_id
				// orders -> input
				_orders.get(w_id).get(d_id).get(no_o_id)._carrier_id = o_carrier_id;
				int c_id = _orders.get(w_id).get(d_id).get(no_o_id)._c_id;

				// updateCustomer : ol_total, c_id, d_id, w_id
				// customers -> input, orders, orderlines
				_customers.get(w_id).get(d_id).get(c_id)._balance += sum;
				// output -> neworders, orderlines
				String result = String
						.format("delivery result: district_id=%d, order_id=%d, top_up=%f",
								d_id, no_o_id, sum);
				_collector.emit(new Values(result));
			}
		}
		// NEW_ORDER
		else if (streamname.equals("NEW_ORDER")) {
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
			for (int i = 0; i < i_ids.size(); ++i) {
				all_local = (all_local && (w_id == i_w_ids.get(i)));
			}

			// getWarehouseTaxRate
			// warehouses -> input
			double w_tax = _warehouses.get(w_id)._tax;

			// getDistrict : d_id, w_id
			// districts -> input
			DistrictState tmpDistrict = _districts.get(w_id).get(d_id);
			double d_tax = tmpDistrict._tax;
			int d_next_o_id = tmpDistrict._next_o_id;
			// incrementNextOrderId : d_next_o_id + 1, d_id, w_id
			tmpDistrict._next_o_id = d_next_o_id + 1;

			// getCustomer : w_id, d_id, c_id
			// customers -> input
			double c_discount = _customers.get(w_id).get(d_id).get(c_id)._discount;

			int ol_cnt = i_ids.size();
			int o_carrier_id = BenchmarkConstant.NULL_CARRIER_ID;
			// createOrder : d_next_o_id, d_id, w_id, c_id, o_entry_d,
			// o_carrier_id, ol_cnt, all_local
			// orders -> input, district
			OrderState orderState = new OrderState(d_next_o_id, c_id, d_id,
					w_id, o_entry_d, o_carrier_id, ol_cnt, all_local);
			_orders.get(w_id).get(d_id).put(d_next_o_id, orderState);

			// createNewOrder : d_next_o_id, d_id, w_id
			// neworders -> input, district
			_neworders.get(w_id).get(d_id).add(d_next_o_id);

			double total = 0;
			for (int i = 0; i < i_ids.size(); ++i) {
				int ol_number = i + 1;
				int ol_supply_w_id = i_w_ids.get(i);
				int ol_i_id = i_ids.get(i);
				int ol_quantity = i_qtys.get(i);

				// getStockInfo : ol_i_id, ol_supply_w_id
				// stocks -> input
				StockState stockInfo = _stocks.get(ol_i_id).get(ol_supply_w_id);
				int s_quantity = stockInfo._quantity;
				int s_ytd = stockInfo._ytd;
				int s_order_cnt = stockInfo._order_cnt;
				int s_remote_cnt = stockInfo._remote_cnt;
				String s_dist = stockInfo._dists.get(d_id - 1);
				s_ytd += ol_quantity;
				if (s_quantity >= ol_quantity + 10) {
					s_quantity = s_quantity - ol_quantity;
				} else {
					s_quantity = s_quantity + 91 - ol_quantity;
				}
				s_order_cnt += 1;
				if (ol_supply_w_id != w_id) {
					s_remote_cnt += 1;
				}
				stockInfo._quantity = s_quantity;
				stockInfo._ytd = s_ytd;
				stockInfo._order_cnt = s_order_cnt;
				stockInfo._remote_cnt = s_remote_cnt;

				// items -> input
				double ol_amount = ol_quantity
						* _items.get(i_ids.get(i))._price;
				total += ol_amount;

				// create new order line
				// orderlines -> neworders, items, input
				OrderLineState olState = new OrderLineState(d_next_o_id, d_id,
						w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d,
						ol_quantity, ol_amount, s_dist);
				if (!_orderlines.get(w_id).get(d_id).containsKey(d_next_o_id)) {
					_orderlines.get(w_id).get(d_id)
							.put(d_next_o_id, new LinkedList<OrderLineState>());
				}
				_orderlines.get(w_id).get(d_id).get(d_next_o_id).add(olState);
			}
			total *= (1 - c_discount) * (1 + w_tax + d_tax);
			// output -> customers, warehouses, districts
			String result = String
					.format("new_order result: customer_id= %d, warehouse_tax=%f, district_tax=%f, order_id=%d, total=%f",
							c_id, w_tax, d_tax, d_next_o_id, total);
			_collector.emit(new Values(result));
		}
		// ORDER_STATUS
		else if (streamname == "ORDER_STATUS") {
			int w_id = Integer.valueOf(fields[0]);
			int d_id = Integer.valueOf(fields[1]);
			int c_id = Integer.valueOf(fields[2]);
			CustomerState tmpCustomer;
			// customers -> input
			if (c_id != -1) {
				tmpCustomer = _customers.get(w_id).get(d_id).get(c_id);
			} else {
				// Get the midpoint customer's id
				List<CustomerState> customerList = new LinkedList<CustomerState>();
				for (CustomerState entry : _customers.get(w_id).get(d_id)
						.values()) {
					if (entry._last.equals(fields[3])) {
						customerList.add(entry);
					}
				}
				tmpCustomer = customerList.get((customerList.size() - 1) / 2);
				c_id = tmpCustomer._id;
			}
			OrderState lastOrder = null;
			// getLastOrder : w_id, d_id, c_id
			// orders -> input, customers
			Map<Integer, OrderState> tmpOrderList = _orders.get(w_id).get(d_id);
			for (OrderState tmpOrder : tmpOrderList.values()) {
				if (tmpOrder._c_id == tmpCustomer._id) {
					lastOrder = tmpOrder;
					break;
				}
			}
			if (lastOrder == null) {
				_collector.emit(new Values("order_status result: null"));
			} else {
				// getOrderLines : w_id, d_id, order[0]
				for (OrderLineState tmpOrderline : _orderlines.get(w_id)
						.get(d_id).get(lastOrder._id)) {
					String result = String
							.format("order_status result: customer_id=%d, last_order_id=%d, item_id=%d, balance=%f",
									tmpCustomer._id, lastOrder._id,
									tmpOrderline._ol_i_id, tmpCustomer._balance);
					_collector.emit(new Values(result));
				}
			}
		}

		// PAYMENT
		else if (streamname == "PAYMENT") {
			int w_id = Integer.valueOf(fields[0]);
			int d_id = Integer.valueOf(fields[1]);
			double h_amount = Double.valueOf(fields[2]);
			int c_w_id = Integer.valueOf(fields[3]);
			int c_d_id = Integer.valueOf(fields[4]);
			int c_id = Integer.valueOf(fields[5]);
			String c_last = fields[6];
			long h_date = Long.valueOf(fields[7]);
			CustomerState tmpCustomer;
			// customers -> input
			if (c_id != -1) {
				tmpCustomer = _customers.get(c_w_id).get(c_d_id).get(c_id);
			} else {
				// Get the midpoint customer's id
				List<CustomerState> customerList = new LinkedList<CustomerState>();
				for (CustomerState entry : _customers.get(c_w_id).get(c_d_id)
						.values()) {
					if (entry._last.equals(c_last)) {
						customerList.add(entry);
					}
				}
				tmpCustomer = customerList.get((customerList.size() - 1) / 2);
				c_id = tmpCustomer._id;
			}
			tmpCustomer._balance -= h_amount;
			tmpCustomer._ytd_payment += h_amount;
			tmpCustomer._payment_count += 1;

			// warehouses -> input
			_warehouses.get(w_id)._ytd += h_amount;
			// districts -> input
			_districts.get(w_id).get(d_id)._ytd += h_amount;

			String h_data = BenchmarkRandom.getAstring(
					BenchmarkConstant.MIN_DATA, BenchmarkConstant.MAX_DATA);
			// InsertHistory
			HistoryState tmpHistory = new HistoryState(tmpCustomer._id, c_d_id,
					c_w_id, d_id, w_id, h_date, h_amount, h_data);
			_histories.add(tmpHistory);
			String result = String
					.format("payment result: warehouse_id=%d, district_id=%d, customer_id=%d, balance=%f, ytd_payment=%f",
							w_id, d_id, tmpCustomer._id, tmpCustomer._balance,
							tmpCustomer._ytd_payment);
			_collector.emit(new Values(result));
		}
		// STOCK_LEVEL
		else if (streamname == "STOCK_LEVEL") {
			int w_id = Integer.valueOf(fields[0]);
			int d_id = Integer.valueOf(fields[1]);
			int threshold = Integer.valueOf(fields[2]);
			// getOId
			// districts -> input
			int next_o_id = _districts.get(w_id).get(d_id)._next_o_id;
			// getStockCount : w_id, d_id, o_id, (o_id-20), w_id, threshold
			// stocks -> orderlines
			for (int o_id = next_o_id - 20; o_id < next_o_id; ++o_id) {
				for (OrderLineState tmpOrderline : _orderlines.get(w_id)
						.get(d_id).get(o_id)) {
					StockState tmpStock = _stocks.get(tmpOrderline._ol_i_id)
							.get(w_id);
					if (tmpStock._quantity < threshold) {
						String result = String
								.format("stock_level result: item_id=%d, warehouse_id=%d, quantity=%d",
										tmpStock._i_id, tmpStock._w_id,
										tmpStock._quantity);
						_collector.emit(new Values(result));
					}
				}
			}
		}

		if (streamname == "DELIVERY" || streamname == "NEW_ORDER"
				|| streamname == "ORDER_STATUS" || streamname == "PAYMENT"
				|| streamname == "STOCK_LEVEL") {
			// event count
			++_numEventCount;
			if (_isFirstQuery) {
				long elapsedTime = System.currentTimeMillis() - _beginTime;
				System.out.println("load database elapsed time = "
						+ elapsedTime + "ms");
				_isFirstQuery = false;
				_beginTime = System.currentTimeMillis();

			} else if (_numEventCount % 20000 == 0) {
				_numEventCount = 0;
				MemoryReport.reportStatus();
				long elapsedTime = System.currentTimeMillis() - _beginTime;
				System.out.println("query database elapsed time = "
						+ elapsedTime + "ms");

				System.out.println("===================================");
				System.out.println("item num=" + _numItems);
				System.out.println("warehouse num=" + _numWarehouses);
				System.out.println("district num=" + _numDistricts);
				System.out.println("customer num=" + _numCustomers);
				System.out.println("order num=" + _numOrders);
				System.out.println("neworder num=" + _numNeworders);
				System.out.println("orderline num=" + _numOrderlines);
				System.out.println("history num=" + _numHistories);
				System.out.println("stock num=" + _numStocks);

				System.out.println("***********************************");
				_beginTime = System.currentTimeMillis();
			}

		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;

		_items = new HashMap<Integer, ItemState>();
		_warehouses = new HashMap<Integer, WarehouseState>();
		_districts = new HashMap<Integer, Map<Integer, DistrictState>>();
		_customers = new HashMap<Integer, Map<Integer, Map<Integer, CustomerState>>>();
		_orders = new HashMap<Integer, Map<Integer, Map<Integer, OrderState>>>();
		_neworders = new HashMap<Integer, Map<Integer, List<Integer>>>();
		_orderlines = new HashMap<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>>();
		_histories = new LinkedList<HistoryState>();
		_stocks = new HashMap<Integer, Map<Integer, StockState>>();

		_beginTime = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
