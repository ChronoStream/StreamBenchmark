package stream.benchmark.tpcc.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
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

public class NewOrderDBBolt extends BaseRichBolt {

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

	private Connection _connection = null;
	private Statement _statement = null;
	private PreparedStatement _itemsInsertion;
	private PreparedStatement _warehousesInsertion;
	private PreparedStatement _districtsInsertion;
	private PreparedStatement _customersInsertion;
	private PreparedStatement _ordersInsertion;
	private PreparedStatement _newordersInsertion;
	private PreparedStatement _orderlinesInsertion;
	private PreparedStatement _historiesInsertion;
	private PreparedStatement _stocksInsertion;

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
				_items.add(item);
				_itemsIndex.put(item_id, item);
//				 _itemsInsertion.setInt(1, Integer.valueOf(fields[0]));
//				 _itemsInsertion.setInt(2, Integer.valueOf(fields[1]));
//				 _itemsInsertion.setString(3, fields[2]);
//				 _itemsInsertion.setDouble(4, Double.valueOf(fields[3]));
//				 _itemsInsertion.setString(5, fields[4]);
//				 _itemsInsertion.addBatch();
//				 _itemsInsertion.executeUpdate();
			}
			// warehouse
			else if (streamname == "warehouse") {
				int warehouse_id = Integer.valueOf(fields[0]);
				WarehouseState warehouse = new WarehouseState(warehouse_id,
						fields[1], fields[2], fields[3], fields[4], fields[5],
						fields[6], Double.valueOf(fields[7]),
						Double.valueOf(fields[8]));
				_warehouses.add(warehouse);
				_warehousesIndex.put(Integer.valueOf(fields[0]), warehouse);

//				 _warehousesInsertion.setInt(1, warehouse_id);
//				 _warehousesInsertion.setString(2, fields[1]);
//				 _warehousesInsertion.setString(3, fields[2]);
//				 _warehousesInsertion.setString(4, fields[3]);
//				 _warehousesInsertion.setString(5, fields[4]);
//				 _warehousesInsertion.setString(6, fields[5]);
//				 _warehousesInsertion.setString(7, fields[6]);
//				 _warehousesInsertion.setDouble(8, Double.valueOf(fields[7]));
//				 _warehousesInsertion.setDouble(9, Double.valueOf(fields[8]));
//				 _warehousesInsertion.addBatch();
//				 _warehousesInsertion.executeUpdate();
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
				_districts.add(district);
				if (!_districtsIndex.containsKey(warehouse_id)) {
					_districtsIndex.put(warehouse_id,
							new HashMap<Integer, DistrictState>());
				}
				_districtsIndex.get(warehouse_id).put(district_id, district);

//				 _districtsInsertion.setInt(1, Integer.valueOf(fields[0]));
//				 _districtsInsertion.setInt(2, Integer.valueOf(fields[1]));
//				 _districtsInsertion.setString(3, fields[2]);
//				 _districtsInsertion.setString(4, fields[3]);
//				 _districtsInsertion.setString(5, fields[4]);
//				 _districtsInsertion.setString(6, fields[5]);
//				 _districtsInsertion.setString(7, fields[6]);
//				 _districtsInsertion.setString(8, fields[7]);
//				 _districtsInsertion.setDouble(9, Double.valueOf(fields[8]));
//				 _districtsInsertion.setDouble(10, Double.valueOf(fields[9]));
//				 _districtsInsertion.setInt(11, Integer.valueOf(fields[10]));
//				 _districtsInsertion.addBatch();
//				 _districtsInsertion.executeUpdate();
			}
			// customer
			else if (streamname == "customer") {
				int customer_id = Integer.valueOf(fields[0]);
				int district_id = Integer.valueOf(fields[1]);
				int warehouse_id = Integer.valueOf(fields[2]);
				CustomerState customer = new CustomerState(customer_id,
						district_id, warehouse_id, fields[3], fields[4],
						fields[5], fields[6], fields[7], fields[8], fields[9],
						fields[10], fields[11], Long.valueOf(fields[12]),
						fields[13], Double.valueOf(fields[14]),
						Double.valueOf(fields[15]), Double.valueOf(fields[16]),
						Double.valueOf(fields[17]),
						Integer.valueOf(fields[18]),
						Integer.valueOf(fields[19]), fields[20]);
				_customers.add(customer);
				if (!_customersIndex.containsKey(warehouse_id)) {
					_customersIndex
							.put(warehouse_id,
									new HashMap<Integer, Map<Integer, CustomerState>>());
				}
				if (!_customersIndex.get(warehouse_id).containsKey(district_id)) {
					_customersIndex.get(warehouse_id).put(district_id,
							new HashMap<Integer, CustomerState>());
				}
				_customersIndex.get(warehouse_id).get(district_id)
						.put(customer_id, customer);

//				 _customersInsertion.setInt(1, Integer.valueOf(fields[0]));
//				 _customersInsertion.setInt(2, Integer.valueOf(fields[1]));
//				 _customersInsertion.setInt(3, Integer.valueOf(fields[2]));
//				 _customersInsertion.setString(4, fields[3]);
//				 _customersInsertion.setString(5, fields[4]);
//				 _customersInsertion.setString(6, fields[5]);
//				 _customersInsertion.setString(7, fields[6]);
//				 _customersInsertion.setString(8, fields[7]);
//				 _customersInsertion.setString(9, fields[8]);
//				 _customersInsertion.setString(10, fields[9]);
//				 _customersInsertion.setString(11, fields[10]);
//				 _customersInsertion.setString(12, fields[11]);
//				 _customersInsertion.setLong(13, Long.valueOf(fields[12]));
//				 _customersInsertion.setString(14, fields[13]);
//				 _customersInsertion.setDouble(15,
//				 Double.valueOf(fields[14]));
//				 _customersInsertion.setDouble(16,
//				 Double.valueOf(fields[15]));
//				 _customersInsertion.setDouble(17,
//				 Double.valueOf(fields[16]));
//				 _customersInsertion.setDouble(18,
//				 Double.valueOf(fields[17]));
//				 _customersInsertion.setInt(19, Integer.valueOf(fields[18]));
//				 _customersInsertion.setInt(20, Integer.valueOf(fields[19]));
//				 _customersInsertion.setString(21, fields[20]);
//				 _customersInsertion.addBatch();
//				 _customersInsertion.executeUpdate();
			}
			// stock
			else if (streamname == "stock") {
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
				StockState stock = new StockState(item_id, warehouse_id,
						quantity, dists, ytd, order_cnt, remote_cnt, data);
				_stocks.add(stock);
				if (!_stocksIndex.containsKey(item_id)) {
					_stocksIndex.put(item_id,
							new HashMap<Integer, StockState>());
				}
				if (!_stocksIndex.get(item_id).containsKey(warehouse_id)) {
					_stocksIndex.get(item_id).put(warehouse_id, stock);
				}

//				 _stocksInsertion.setInt(1, Integer.valueOf(fields[0]));
//				 _stocksInsertion.setInt(2, Integer.valueOf(fields[1]));
//				 _stocksInsertion.setInt(3, Integer.valueOf(fields[2]));
//				 tmpId = 3;
//				 for (; tmpId < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE + 3;
//				 ++tmpId) {
//				 _stocksInsertion.setString(tmpId + 1, fields[tmpId]);
//				 }
//				 _stocksInsertion.setInt(tmpId + 1,
//				 Integer.valueOf(fields[tmpId]));
//				 ++tmpId;
//				 _stocksInsertion.setInt(tmpId + 1,
//				 Integer.valueOf(fields[tmpId]));
//				 ++tmpId;
//				 _stocksInsertion.setInt(tmpId + 1,
//				 Integer.valueOf(fields[tmpId]));
//				 ++tmpId;
//				 _stocksInsertion.setString(tmpId + 1, fields[tmpId]);
//				 _stocksInsertion.addBatch();
//				 _stocksInsertion.executeUpdate();
			}
			// order
			else if (streamname == "order") {
				int order_id = Integer.valueOf(fields[0]);
				int customer_id = Integer.valueOf(fields[1]);
				int district_id = Integer.valueOf(fields[2]);
				int warehouse_id = Integer.valueOf(fields[3]);
				OrderState order = new OrderState(order_id, customer_id,
						district_id, warehouse_id, Long.valueOf(fields[4]),
						Integer.valueOf(fields[5]), Double.valueOf(fields[6]),
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
//				 _ordersInsertion.setInt(1, Integer.valueOf(fields[0]));
//				 _ordersInsertion.setInt(2, Integer.valueOf(fields[1]));
//				 _ordersInsertion.setInt(3, Integer.valueOf(fields[2]));
//				 _ordersInsertion.setInt(4, Integer.valueOf(fields[3]));
//				 _ordersInsertion.setLong(5, Long.valueOf(fields[4]));
//				 _ordersInsertion.setInt(6, Integer.valueOf(fields[5]));
//				 _ordersInsertion.setDouble(7, Double.valueOf(fields[6]));
//				 _ordersInsertion.setBoolean(8, Boolean.valueOf(fields[7]));
//				 _ordersInsertion.addBatch();
//				 _ordersInsertion.executeUpdate();
			}
			// neworder
			else if (streamname == "neworder") {
				int order_id = Integer.valueOf(fields[0]);
				int district_id = Integer.valueOf(fields[1]);
				int warehouse_id = Integer.valueOf(fields[2]);
				NewOrderState neworder = new NewOrderState(order_id,
						district_id, warehouse_id);
				_neworders.add(neworder);
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

//				 _newordersInsertion.setInt(1, Integer.valueOf(fields[0]));
//				 _newordersInsertion.setInt(2, Integer.valueOf(fields[1]));
//				 _newordersInsertion.setInt(3, Integer.valueOf(fields[2]));
//				 _newordersInsertion.addBatch();
//				 _newordersInsertion.executeUpdate();
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
				_orderlines.add(orderline);
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

//				 _orderlinesInsertion.setInt(1, Integer.valueOf(fields[0]));
//				 _orderlinesInsertion.setInt(2, Integer.valueOf(fields[1]));
//				 _orderlinesInsertion.setInt(3, Integer.valueOf(fields[2]));
//				 _orderlinesInsertion.setInt(4, Integer.valueOf(fields[3]));
//				 _orderlinesInsertion.setInt(5, Integer.valueOf(fields[4]));
//				 _orderlinesInsertion.setInt(6, Integer.valueOf(fields[5]));
//				 _orderlinesInsertion.setLong(7, Long.valueOf(fields[6]));
//				 _orderlinesInsertion.setInt(8, Integer.valueOf(fields[7]));
//				 _orderlinesInsertion.setDouble(9, Double.valueOf(fields[8]));
//				 _orderlinesInsertion.setString(10, fields[9]);
//				 _orderlinesInsertion.addBatch();
//				 _orderlinesInsertion.executeUpdate();
			}
			// history
			else if (streamname == "history") {
				int h_c_id = Integer.valueOf(fields[0]);
				int h_c_d_id = Integer.valueOf(fields[1]);
				int h_c_w_id = Integer.valueOf(fields[2]);
				HistoryState history = new HistoryState(h_c_id, h_c_d_id,
						h_c_w_id, Integer.valueOf(fields[3]),
						Integer.valueOf(fields[4]), Long.valueOf(fields[5]),
						Double.valueOf(fields[6]), fields[7]);
				_histories.add(history);
				if (!_historiesIndex.containsKey(h_c_w_id)) {
					_historiesIndex.put(h_c_w_id,
							new HashMap<Integer, Map<Integer, HistoryState>>());
				}
				if (!_historiesIndex.get(h_c_w_id).containsKey(h_c_d_id)) {
					_historiesIndex.get(h_c_w_id).put(h_c_d_id,
							new HashMap<Integer, HistoryState>());
				}
				_historiesIndex.get(h_c_w_id).get(h_c_d_id)
						.put(h_c_id, history);
//				 _historiesInsertion.setInt(1, Integer.valueOf(fields[0]));
//				 _historiesInsertion.setInt(2, Integer.valueOf(fields[1]));
//				 _historiesInsertion.setInt(3, Integer.valueOf(fields[2]));
//				 _historiesInsertion.setInt(4, Integer.valueOf(fields[3]));
//				 _historiesInsertion.setInt(5, Integer.valueOf(fields[4]));
//				 _historiesInsertion.setLong(6, Long.valueOf(fields[5]));
//				 _historiesInsertion.setDouble(7, Double.valueOf(fields[6]));
//				 _historiesInsertion.setString(8, fields[7]);
//				 _historiesInsertion.addBatch();
//				 _historiesInsertion.executeUpdate();

			} else if (streamname == "DELIVERY") {
			} else if (streamname == "NEW_ORDER") {
				// ===================heap=======================
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
				// ==============================================
				boolean all_local = true;
				// selection and projection
				List<NewOrderItemInfo> item_infos = new LinkedList<NewOrderItemInfo>();
				
				for (int i = 0; i < i_ids.size(); ++i) {
					all_local = (all_local && (w_id == i_w_ids.get(i)));
//					// get from state
//					// ********** select i_price, i_name, i_data from item where i_id = ?
//					// DATABASE VERSION
//					ResultSet result = _statement.executeQuery("select i_price, i_name, i_data from items where i_id =" + i_ids.get(i));
//					result.next();
//					item_infos.add(new NewOrderItemInfo(result.getDouble(1), result.getString(2), result.getString(3)));
//					// STATE VERSION
					ItemState tmpItem = _itemsIndex.get(i_ids.get(i));
					item_infos.add(new NewOrderItemInfo(tmpItem._price,
							tmpItem._name, tmpItem._data));
				}
				
				// get from state
				// getWarehouseTaxRate
				// ********** select w_tax from warehouse where w_id = ?
				// DATABASE VERSION
//				ResultSet result = _statement.executeQuery("select w_tax from warehouses where w_id =" + w_id);
//				result.next();
//				double w_tax = result.getDouble(1);
				// STATE VERSION
				double w_tax = _warehousesIndex.get(w_id)._tax;
				
				// get from state, set to state
				// getDistrict : d_id, w_id
				// ********* select d_tax, d_next_o_id from district where d_id = ? and d_w_id = ?
				// DATABASE VERSION
//				ResultSet result = _statement.executeQuery("select d_tax, d_next_o_id from districts where d_id = "+ d_id + " and d_w_id = " + w_id);
//				result.next();
//				double d_tax = result.getDouble(1);
//				int d_next_o_id = result.getInt(2);
				// STATE VERSION
				DistrictState tmpDistrict = _districtsIndex.get(w_id).get(d_id);
				double d_tax = tmpDistrict._tax;
				int d_next_o_id = tmpDistrict._next_o_id;
				
				// incrementNextOrderId : d_next_o_id + 1
				// ********** update district set d_next_o_id = ? where d_id = ? and d_w_id = ?
				// DATABASE VERSION
//				_statement.executeUpdate("update districts set d_next_o_id="+ (d_next_o_id + 1) + " where d_id = " + d_id + " and d_w_id = " + w_id);
				// STATE VERSION
				tmpDistrict._next_o_id = d_next_o_id + 1;

				// get from state
				// getCustomer : w_id, d_id, c_id
				// *********** select c_discount, c_last, c_credit from customer where c_w_id = ? and c_d_id = ? and c_id = ?
				// DATABASE VERSION
//				ResultSet result = _statement.executeQuery("select c_discount, c_last, c_credit from customers where c_w_id = "+ w_id +" and c_d_id = " + d_id +" and c_id= " + c_id);
//				result.next();
//				double c_discount = result.getDouble(1);
				// STATE VERSION
				double c_discount = _customersIndex.get(w_id).get(d_id)
						.get(c_id)._discount;
				
				int ol_cnt = i_ids.size();
				int o_carrier_id = BenchmarkConstant.NULL_CARRIER_ID;

				// append to state
				// createOrder : d_next_o_id, d_id, w_id, c_id, o_entry_d,
				// o_carrier_id, ol_cnt, all_local
				// ************ insert into orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) values (?, ?, ?, ?, ?, ?, ?, ?)
				// DATABASE VERSION
				 _ordersInsertion.setInt(1, d_next_o_id);
				 _ordersInsertion.setInt(2, d_id);
				 _ordersInsertion.setInt(3, w_id);
				 _ordersInsertion.setInt(4, c_id);
				 _ordersInsertion.setLong(5, o_entry_d);
				 _ordersInsertion.setInt(6, o_carrier_id);
				 _ordersInsertion.setDouble(7, ol_cnt);
				 _ordersInsertion.setBoolean(8, all_local);
				 _ordersInsertion.addBatch();
				 _ordersInsertion.executeUpdate();
				// STATE VERSION
//				OrderState orderState = new OrderState(d_next_o_id, d_id, w_id,
//						c_id, o_entry_d, o_carrier_id, ol_cnt, all_local);
//				_orders.add(orderState);
//				_ordersIndex.get(w_id).get(d_id).put(d_next_o_id, orderState);
				
				// append to state
				// createNewOrder : d_next_o_id, d_id, w_id
				// ***********
				NewOrderState neworderState = new NewOrderState(d_next_o_id,
						d_id, w_id);
				_neworders.add(neworderState);
				_newordersIndex.get(w_id).get(d_id).add(d_next_o_id);
				// =============================================================

				List<NewOrderItemData> item_data = new LinkedList<NewOrderItemData>();
				double total = 0;
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
					String s_data = stockInfo._data;
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

					String brand_generic;
					if (tmpItemInfo._i_data
							.contains(BenchmarkConstant.ORIGINAL_STRING) == true
							&& s_data
									.contains(BenchmarkConstant.ORIGINAL_STRING) == true) {
						brand_generic = "B";
					} else {
						brand_generic = "G";
					}
					double ol_amount = ol_quantity * tmpItemInfo._i_price;
					total += ol_amount;
					// create new order line
					OrderLineState olState = new OrderLineState(d_next_o_id,
							d_id, w_id, ol_number, ol_i_id, ol_supply_w_id,
							o_entry_d, ol_quantity, ol_amount, s_dist);
					_orderlines.add(olState);
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
					item_data.add(new NewOrderItemData(tmpItemInfo._i_name,
							s_quantity, brand_generic, tmpItemInfo._i_price,
							ol_amount));
				}
				total *= (1 - c_discount) * (1 + w_tax + d_tax);
				_collector.emit(new Values("total=" + total));
				
			} else if (streamname == "ORDER_STATUS") {
			} else if (streamname == "PAYMENT") {
			} else if (streamname == "STOCK_LEVEL") {
			} else {
			}
			// _collector.emit(new Values(streamname));
		} catch (Exception e) {
			e.printStackTrace();
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
		_orderlines = new LinkedList<OrderLineState>();
		_orderlinesIndex = new HashMap<Integer, Map<Integer, Map<Integer, List<OrderLineState>>>>();
		_histories = new LinkedList<HistoryState>();
		_historiesIndex = new HashMap<Integer, Map<Integer, Map<Integer, HistoryState>>>();
		_stocks = new LinkedList<StockState>();
		_stocksIndex = new HashMap<Integer, Map<Integer, StockState>>();
		databaseInit();
	}

	protected void databaseInit() {
		try {
			_connection = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/tpcc", "root", "");
			_statement = _connection.createStatement();
//			_statement.executeUpdate("drop table warehouses");
//			_statement.executeUpdate("drop table districts");
//			_statement.executeUpdate("drop table customers");
//			_statement.executeUpdate("drop table orders");
//			_statement.executeUpdate("drop table neworders");
//			_statement.executeUpdate("drop table orderlines");
//			_statement.executeUpdate("drop table histories");
//			_statement.executeUpdate("drop table stocks");
//			_statement.executeUpdate("drop table items");
//			_statement
//					.executeUpdate("create table warehouses"
//							+ "(w_id smallint, w_name varchar(16), "
//							+ "w_street_1 varchar(32), w_street_2 varchar(32), w_city varchar(32), w_state varchar(2), w_zip varchar(9), "
//							+ "w_tax float, w_ytd float) " + "engine=memory");
//			_statement
//					.executeUpdate("create index warehousesindex on warehouses(w_id)");
//			_statement
//					.executeUpdate("create table districts"
//							+ "(d_id smallint, d_w_id smallint, d_name varchar(16), "
//							+ "d_street_1 varchar(32), d_street_2 varchar(32), d_city varchar(32), d_state varchar(2), d_zip varchar(9), "
//							+ "d_tax float, d_ytd float, d_next_o_id int) "
//							+ "engine=memory");
//			_statement
//					.executeUpdate("create index districtsindex on districts(d_w_id, d_id)");
//			_statement
//					.executeUpdate("create table items"
//							+ "(i_id int, i_im_id int, i_name varchar(32), i_price float, i_data varchar(64))  "
//							+ "engine=memory");
//			_statement.executeUpdate("create index itemsindex on items(i_id)");
//			_statement
//					.executeUpdate("create table customers"
//							+ "(c_id int, c_d_id smallint, c_w_id smallint, "
//							+ "c_first varchar(32), c_middle varchar(2), c_last varchar(32), "
//							+ "c_street_1 varchar(32), c_street_2 varchar(32), c_city varchar(32), c_state varchar(2), c_zip varchar(9), "
//							+ "c_phone varchar(32), c_since bigint, "
//							+ "c_credit varchar(2), c_credit_lim float, "
//							+ "c_discount float, c_balance float, c_ytd_payment float, c_payment_cnt int, c_delivery_cnt int, "
//							+ "c_data varchar(500)) " + "engine=memory");
//			_statement
//					.executeUpdate("create index customersindex on customers(c_w_id, c_d_id, c_id)");
//			_statement
//					.executeUpdate("create table orders"
//							+ "(o_id int, o_c_id int, o_d_id smallint, o_w_id smallint, o_entry_d bigint, o_carrier_id int, o_ol_cnt int, o_all_local int) "
//							+ "engine=memory");
//			_statement
//					.executeUpdate("create index ordersindex on orders(o_w_id, o_d_id, o_id)");
//			_statement.executeUpdate("create table neworders"
//					+ "(no_o_id int, no_d_id smallint, no_w_id smallint) "
//					+ "engine=memory");
//			_statement
//					.executeUpdate("create index newordersindex on neworders(no_d_id, no_w_id, no_o_id)");
//			_statement
//					.executeUpdate("create table orderlines"
//							+ "(ol_o_id int, ol_d_id smallint, ol_w_id smallint, "
//							+ "ol_number int, ol_i_id int, ol_supply_w_id smallint, ol_delivery_d bigint, ol_quantity int, ol_amount float, ol_dist_info varchar(32)) "
//							+ "engine=memory");
//			_statement
//					.executeUpdate("create index orderlinesindex on orderlines(ol_w_id, ol_d_id, ol_o_id, ol_number)");
//			_statement
//					.executeUpdate("create table histories"
//							+ "(h_c_id int, h_c_d_id smallint, h_c_w_id smallint, h_d_id smallint, h_w_id smallint, h_date bigint, h_amount float, h_data varchar(32)) "
//							+ "engine=memory");
//			_statement
//					.executeUpdate("create index historiesindex on histories(h_d_id, h_w_id)");
//			_statement
//					.executeUpdate("create table stocks"
//							+ "(s_i_id int, s_w_id smallint, s_quantity int, "
//							+ "s_dist_01 varchar(32), s_dist_02 varchar(32), s_dist_03 varchar(32), s_dist_04 varchar(32), s_dist_05 varchar(32), "
//							+ "s_dist_06 varchar(32), s_dist_07 varchar(32), s_dist_08 varchar(32), s_dist_09 varchar(32), s_dist_10 varchar(32), "
//							+ "s_ytd int, s_order_cnt int, s_remote_cnt int, s_data varchar(64)) "
//							+ "engine=memory");
//			_statement
//					.executeUpdate("create index stocksindex on stocks(s_w_id, s_i_id)");

			_itemsInsertion = _connection
					.prepareStatement("insert into items values(?, ?, ?, ?, ?)");
			_warehousesInsertion = _connection
					.prepareStatement("insert into warehouses values(?, ?, ?, ?, ?, ?, ?, ?, ?)");
			_districtsInsertion = _connection
					.prepareStatement("insert into districts values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			_customersInsertion = _connection
					.prepareStatement("insert into customers values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			_ordersInsertion = _connection
					.prepareStatement("insert into orders values(?, ?, ?, ?, ?, ?, ?, ?)");
			_newordersInsertion = _connection
					.prepareStatement("insert into neworders values(?, ?, ?)");
			_orderlinesInsertion = _connection
					.prepareStatement("insert into orderlines values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			_historiesInsertion = _connection
					.prepareStatement("insert into histories values(?, ?, ?, ?, ?, ?, ?, ?)");
			_stocksInsertion = _connection
					.prepareStatement("insert into stocks values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

			System.out.println("database initiated!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
