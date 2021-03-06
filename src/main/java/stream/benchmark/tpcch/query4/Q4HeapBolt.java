package stream.benchmark.tpcch.query4;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.toolkits.MemoryReport;
import stream.benchmark.tpcch.query.InnerState.NewOrderItemInfo;
import stream.benchmark.tpcch.query.TableState.HistoryState;
import stream.benchmark.tpcch.query.TableState.ItemState;
import stream.benchmark.tpcch.query.TableState.NationState;
import stream.benchmark.tpcch.query.TableState.NewOrderState;
import stream.benchmark.tpcch.query.TableState.OrderLineState;
import stream.benchmark.tpcch.query.TableState.OrderState;
import stream.benchmark.tpcch.query.TableState.RegionState;
import stream.benchmark.tpcch.query.TableState.StockState;
import stream.benchmark.tpcch.query.TableState.SupplierState;
import stream.benchmark.tpcch.query.TableState.WarehouseState;
import stream.benchmark.tpcch.query.TableState.DistrictState;
import stream.benchmark.tpcch.query.TableState.CustomerState;
import stream.benchmark.tpcch.spout.BenchmarkConstant;
import stream.benchmark.tpcch.spout.BenchmarkRandom;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Q4HeapBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private class ResultState {
		public ResultState(int _o_ol_cnt, int _order_count) {
			this._o_ol_cnt = _o_ol_cnt;
			this._order_count = _order_count;
		}

		int _o_ol_cnt;
		int _order_count;
	}

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
	private Map<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>> _orderlinesIndex;
	private List<HistoryState> _histories;
	private Map<Integer, Map<Integer, Map<Integer, List<HistoryState>>>> _historiesIndex;
	private List<StockState> _stocks;
	private Map<Integer, Map<Integer, StockState>> _stocksIndex;

	private List<NationState> _nations;
	private Map<Integer, NationState> _nationsIndex;

	private List<RegionState> _regions;
	private Map<Integer, RegionState> _regionsIndex;

	private List<SupplierState> _suppliers;
	private Map<Integer, SupplierState> _suppliersIndex;

	private boolean _isFirstQuery = true;
	private long _beginTime;

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
					Integer.valueOf(fields[5]), Integer.valueOf(fields[6]),
					Boolean.valueOf(fields[7]));
			_orders.add(order);
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
				_historiesIndex
						.put(h_c_w_id,
								new HashMap<Integer, Map<Integer, List<HistoryState>>>());
			}
			if (!_historiesIndex.get(h_c_w_id).containsKey(h_c_d_id)) {
				_historiesIndex.get(h_c_w_id).put(h_c_d_id,
						new HashMap<Integer, List<HistoryState>>());
			}
			if (!_historiesIndex.get(h_c_w_id).get(h_c_d_id)
					.containsKey(h_c_id)) {
				_historiesIndex.get(h_c_w_id).get(h_c_d_id)
						.put(h_c_id, new LinkedList<HistoryState>());
			}

			_historiesIndex.get(h_c_w_id).get(h_c_d_id).get(h_c_id)
					.add(history);
		} else if (streamname == "nation") {
			int n_id = Integer.valueOf(fields[0]);
			String n_name = fields[1];
			int r_id = Integer.valueOf(fields[2]);
			NationState nation = new NationState(n_id, n_name, r_id);
			_nations.add(nation);
			_nationsIndex.put(n_id, nation);
		} else if (streamname == "region") {
			int r_id = Integer.valueOf(fields[0]);
			String r_name = fields[1];
			RegionState region = new RegionState(r_id, r_name);
			_regions.add(region);
			_regionsIndex.put(r_id, region);
		} else if (streamname == "supplier") {
			int su_id = Integer.valueOf(fields[0]);
			String su_name = fields[1];
			String su_address = fields[2];
			int n_id = Integer.valueOf(fields[3]);
			SupplierState supplier = new SupplierState(su_id, su_name,
					su_address, n_id);
			_suppliers.add(supplier);
			_suppliersIndex.put(su_id, supplier);
		} 
		// DELIVERY
		else if (streamname == "DELIVERY") {
			int w_id = Integer.valueOf(fields[0]);
			long ol_delivery_d = Long.valueOf(fields[2]);
			// for each district, deliver the first new_order
			for (int d_id = 1; d_id < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE + 1; ++d_id) {
				// randomly pick a new order
				if (_newordersIndex.get(w_id).get(d_id).size() == 0) {
					continue;
				}
				// getNewOrder: no_d_id, no_w_id
				int no_o_id = _newordersIndex.get(w_id).get(d_id).get(0);
				// getCId: no_o_id, d_id, w_id
				int c_id = _ordersIndex.get(w_id).get(d_id).get(no_o_id)._c_id;
				// sumOLAmount: no_o_id, d_id, w_id
				double sum = 0;
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
				// updateOrderLine : ol_delivery_d, no_o_id, d_id, w_id
				List<OrderLineState> tmpList = _orderlinesIndex.get(w_id)
						.get(d_id).get(no_o_id);
				for (OrderLineState tmp : tmpList) {
					tmp._ol_delivery_d = ol_delivery_d;
				}
			}
		} 
		// NEW_ORDER
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

			boolean all_local = true;
			List<NewOrderItemInfo> item_infos = new LinkedList<NewOrderItemInfo>();
			for (int i = 0; i < i_ids.size(); ++i) {
				all_local = (all_local && (w_id == i_w_ids.get(i)));
				ItemState tmpItem = _itemsIndex.get(i_ids.get(i));
				item_infos.add(new NewOrderItemInfo(tmpItem._price,
						tmpItem._name, tmpItem._data));
			}
			// getDistrict : d_id, w_id
			DistrictState tmpDistrict = _districtsIndex.get(w_id).get(d_id);
			int d_next_o_id = tmpDistrict._next_o_id;

			int ol_cnt = i_ids.size();
			int o_carrier_id = BenchmarkConstant.NULL_CARRIER_ID;

			// incrementNextOrderId : d_next_o_id + 1, d_id, w_id
			_districtsIndex.get(w_id).get(d_id)._next_o_id = d_next_o_id + 1;

			// createOrder : d_next_o_id, d_id, w_id, c_id, o_entry_d,
			// o_carrier_id, ol_cnt, all_local
			OrderState orderState = new OrderState(d_next_o_id, c_id, d_id,
					w_id, o_entry_d, o_carrier_id, ol_cnt, all_local);
			_orders.add(orderState);
			_ordersIndex.get(w_id).get(d_id).put(d_next_o_id, orderState);
			// createNewOrder : d_next_o_id, d_id, w_id
			NewOrderState neworderState = new NewOrderState(d_next_o_id, d_id,
					w_id);
			_neworders.add(neworderState);
			_newordersIndex.get(w_id).get(d_id).add(d_next_o_id);

			for (int i = 0; i < i_ids.size(); ++i) {
				int ol_number = i + 1;
				int ol_supply_w_id = i_w_ids.get(i);
				int ol_i_id = i_ids.get(i);
				int ol_quantity = i_qtys.get(i);
				NewOrderItemInfo tmpItemInfo = item_infos.get(i);

				// getStockInfo : ol_i_id, ol_supply_w_id
				StockState stockInfo = _stocksIndex.get(ol_i_id).get(
						ol_supply_w_id);
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

				double ol_amount = ol_quantity * tmpItemInfo._i_price;
				// create new order line
				OrderLineState olState = new OrderLineState(d_next_o_id, d_id,
						w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d,
						ol_quantity, ol_amount, s_dist);
				if (!_orderlinesIndex.get(w_id).get(d_id)
						.containsKey(d_next_o_id)) {
					_orderlinesIndex.get(w_id).get(d_id)
							.put(d_next_o_id, new LinkedList<OrderLineState>());
				}
				_orderlinesIndex.get(w_id).get(d_id).get(d_next_o_id)
						.add(olState);
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
			if (c_id != -1) {
				tmpCustomer = _customersIndex.get(c_w_id).get(c_d_id).get(c_id);
			} else {
				// Get the midpoint customer's id
				List<CustomerState> customerList = new LinkedList<CustomerState>();
				for (CustomerState entry : _customersIndex.get(c_w_id)
						.get(c_d_id).values()) {
					if (entry._last.equals(c_last)) {
						customerList.add(entry);
					}
				}
				tmpCustomer = customerList.get((customerList.size() - 1) / 2);
				c_id = tmpCustomer._id;
			}

			double c_balance = tmpCustomer._balance - h_amount;
			double c_ytd_payment = tmpCustomer._ytd_payment + h_amount;
			int c_payment_cnt = tmpCustomer._payment_count + 1;
			WarehouseState tmpWarehouse = _warehousesIndex.get(w_id);
			DistrictState tmpDistrict = _districtsIndex.get(w_id).get(d_id);
			tmpWarehouse._ytd += h_amount;
			tmpDistrict._ytd += h_amount;

			tmpCustomer._balance = c_balance;
			tmpCustomer._ytd_payment = c_ytd_payment;
			tmpCustomer._payment_count = c_payment_cnt;

			String h_data = BenchmarkRandom.getAstring(
					BenchmarkConstant.MIN_DATA, BenchmarkConstant.MAX_DATA);
			// InsertHistory
			HistoryState tmpHistory = new HistoryState(tmpCustomer._id, c_d_id,
					c_w_id, d_id, w_id, h_date, h_amount, h_data);
			_histories.add(tmpHistory);
			_historiesIndex.get(c_w_id).get(c_d_id).get(c_id).add(tmpHistory);
		}

		if (streamname == "DELIVERY" || streamname == "NEW_ORDER"
				|| streamname == "PAYMENT") {
			if (_isFirstQuery) {
				long elapsedTime = System.currentTimeMillis() - _beginTime;
				System.out.println("load database elapsed time = "
						+ elapsedTime + "ms");
				_isFirstQuery = false;
				_beginTime = System.currentTimeMillis();

			} else if (System.currentTimeMillis() - _beginTime >= 2000) {
				MemoryReport.reportStatus();

				StringBuilder sb = new StringBuilder();

				Map<Integer, ResultState> results = new HashMap<Integer, ResultState>();
				for (OrderState order : _orders) {
					if (System.currentTimeMillis() - order._entry_d < 10000) {
						boolean isDelivered = false;
						for (OrderLineState oltmp : _orderlinesIndex
								.get(order._w_id).get(order._d_id)
								.get(order._id)) {
							if (oltmp._ol_delivery_d > order._entry_d) {
								isDelivered = true;
								break;
							}
						}
						if (isDelivered) {
							int ol_cnt = order._ol_cnt;
							if (!results.containsKey(ol_cnt)) {
								results.put(ol_cnt, new ResultState(ol_cnt, 0));
							}
							results.get(ol_cnt)._order_count += 1;
						}
					}
				}

				for (ResultState result : results.values()) {
					sb.append(result._o_ol_cnt);
					sb.append(", ");
					sb.append(result._order_count);
					_collector.emit(new Values(sb.toString()));
					sb.setLength(0);
				}

				_beginTime = System.currentTimeMillis();
			}
		}
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
		_orderlinesIndex = new HashMap<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>>();
		_histories = new LinkedList<HistoryState>();
		_historiesIndex = new HashMap<Integer, Map<Integer, Map<Integer, List<HistoryState>>>>();
		_stocks = new LinkedList<StockState>();
		_stocksIndex = new HashMap<Integer, Map<Integer, StockState>>();

		_nations = new LinkedList<NationState>();
		_nationsIndex = new HashMap<Integer, NationState>();
		_regions = new LinkedList<RegionState>();
		_regionsIndex = new HashMap<Integer, RegionState>();
		_suppliers = new LinkedList<SupplierState>();
		_suppliersIndex = new HashMap<Integer, SupplierState>();

		_beginTime = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
