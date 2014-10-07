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

import stream.benchmark.toolkits.MemoryReport;
import stream.benchmark.tpcc.query.InnerState.InnerCustomerState;
import stream.benchmark.tpcc.query.InnerState.InnerStockState;
import stream.benchmark.tpcc.spout.BenchmarkConstant;
import stream.benchmark.tpcc.spout.BenchmarkRandom;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StateMachineFissionDBBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;

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

	private boolean _isFirstQuery = true;
	private long _beginTime;

	private int _numEventCount = 0;

	public void execute(Tuple input) {
		try {
			String tuple = input.getString(0);
			String[] fields = tuple.split(",");
			String streamname = input.getSourceStreamId();

			// item
			if (streamname == "item") {
				_itemsInsertion.setInt(1, Integer.valueOf(fields[0]));
				_itemsInsertion.setInt(2, Integer.valueOf(fields[1]));
				_itemsInsertion.setString(3, fields[2]);
				_itemsInsertion.setDouble(4, Double.valueOf(fields[3]));
				_itemsInsertion.setString(5, fields[4]);
				_itemsInsertion.executeUpdate();
			}

			// warehouse
			else if (streamname == "warehouse") {
				_warehousesInsertion.setInt(1, Integer.valueOf(fields[0]));
				_warehousesInsertion.setString(2, fields[1]);
				_warehousesInsertion.setString(3, fields[2]);
				_warehousesInsertion.setString(4, fields[3]);
				_warehousesInsertion.setString(5, fields[4]);
				_warehousesInsertion.setString(6, fields[5]);
				_warehousesInsertion.setString(7, fields[6]);
				_warehousesInsertion.setDouble(8, Double.valueOf(fields[7]));
				_warehousesInsertion.setDouble(9, Double.valueOf(fields[8]));
				_warehousesInsertion.executeUpdate();
			}

			// district
			else if (streamname == "district") {
				_districtsInsertion.setInt(1, Integer.valueOf(fields[0]));
				_districtsInsertion.setInt(2, Integer.valueOf(fields[1]));
				_districtsInsertion.setString(3, fields[2]);
				_districtsInsertion.setString(4, fields[3]);
				_districtsInsertion.setString(5, fields[4]);
				_districtsInsertion.setString(6, fields[5]);
				_districtsInsertion.setString(7, fields[6]);
				_districtsInsertion.setString(8, fields[7]);
				_districtsInsertion.setDouble(9, Double.valueOf(fields[8]));
				_districtsInsertion.setDouble(10, Double.valueOf(fields[9]));
				_districtsInsertion.setInt(11, Integer.valueOf(fields[10]));
				_districtsInsertion.executeUpdate();
			}

			// customer
			else if (streamname == "customer") {
				_customersInsertion.setInt(1, Integer.valueOf(fields[0]));
				_customersInsertion.setInt(2, Integer.valueOf(fields[1]));
				_customersInsertion.setInt(3, Integer.valueOf(fields[2]));
				_customersInsertion.setString(4, fields[3]);
				_customersInsertion.setString(5, fields[4]);
				_customersInsertion.setString(6, fields[5]);
				_customersInsertion.setString(7, fields[6]);
				_customersInsertion.setString(8, fields[7]);
				_customersInsertion.setString(9, fields[8]);
				_customersInsertion.setString(10, fields[9]);
				_customersInsertion.setString(11, fields[10]);
				_customersInsertion.setString(12, fields[11]);
				_customersInsertion.setLong(13, Long.valueOf(fields[12]));
				_customersInsertion.setString(14, fields[13]);
				_customersInsertion.setDouble(15, Double.valueOf(fields[14]));
				_customersInsertion.setDouble(16, Double.valueOf(fields[15]));
				_customersInsertion.setDouble(17, Double.valueOf(fields[16]));
				_customersInsertion.setDouble(18, Double.valueOf(fields[17]));
				_customersInsertion.setInt(19, Integer.valueOf(fields[18]));
				_customersInsertion.setInt(20, Integer.valueOf(fields[19]));
				_customersInsertion.setString(21, fields[20]);
				_customersInsertion.executeUpdate();
			}

			// stock
			else if (streamname == "stock") {
				_stocksInsertion.setInt(1, Integer.valueOf(fields[0]));
				_stocksInsertion.setInt(2, Integer.valueOf(fields[1]));
				_stocksInsertion.setInt(3, Integer.valueOf(fields[2]));
				int tmpId = 3;
				for (; tmpId < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE + 3; ++tmpId) {
					_stocksInsertion.setString(tmpId + 1, fields[tmpId]);
				}
				_stocksInsertion.setInt(tmpId + 1,
						Integer.valueOf(fields[tmpId]));
				++tmpId;
				_stocksInsertion.setInt(tmpId + 1,
						Integer.valueOf(fields[tmpId]));
				++tmpId;
				_stocksInsertion.setInt(tmpId + 1,
						Integer.valueOf(fields[tmpId]));
				++tmpId;
				_stocksInsertion.setString(tmpId + 1, fields[tmpId]);
				_stocksInsertion.executeUpdate();
			}

			// order
			else if (streamname == "order") {
				_ordersInsertion.setInt(1, Integer.valueOf(fields[0]));
				_ordersInsertion.setInt(2, Integer.valueOf(fields[1]));
				_ordersInsertion.setInt(3, Integer.valueOf(fields[2]));
				_ordersInsertion.setInt(4, Integer.valueOf(fields[3]));
				_ordersInsertion.setLong(5, Long.valueOf(fields[4]));
				_ordersInsertion.setInt(6, Integer.valueOf(fields[5]));
				_ordersInsertion.setDouble(7, Double.valueOf(fields[6]));
				_ordersInsertion.setBoolean(8, Boolean.valueOf(fields[7]));
				_ordersInsertion.executeUpdate();
			}

			// neworder
			else if (streamname == "neworder") {
				_newordersInsertion.setInt(1, Integer.valueOf(fields[0]));
				_newordersInsertion.setInt(2, Integer.valueOf(fields[1]));
				_newordersInsertion.setInt(3, Integer.valueOf(fields[2]));
				_newordersInsertion.executeUpdate();
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
				_orderlinesInsertion.executeUpdate();
			}

			// history
			else if (streamname == "history") {
				_historiesInsertion.setInt(1, Integer.valueOf(fields[0]));
				_historiesInsertion.setInt(2, Integer.valueOf(fields[1]));
				_historiesInsertion.setInt(3, Integer.valueOf(fields[2]));
				_historiesInsertion.setInt(4, Integer.valueOf(fields[3]));
				_historiesInsertion.setInt(5, Integer.valueOf(fields[4]));
				_historiesInsertion.setLong(6, Long.valueOf(fields[5]));
				_historiesInsertion.setDouble(7, Double.valueOf(fields[6]));
				_historiesInsertion.setString(8, fields[7]);
				_historiesInsertion.executeUpdate();
			}

			// DELIVERY
			else if (streamname.equals("DELIVERY")) {
				int w_id = Integer.valueOf(fields[0]);
				int o_carrier_id = Integer.valueOf(fields[1]);
				long ol_delivery_d = Long.valueOf(fields[2]);

				// for each district, deliver the first new_order
				Map<Integer, Integer> no_o_ids = new HashMap<Integer, Integer>();
				// getNewOrder: no_d_id, no_w_id
				ResultSet neworderResult = _statement
						.executeQuery("select no_d_id, no_o_id from neworders where no_d_id >= 1 and no_d_id <= "
								+ BenchmarkConstant.DISTRICTS_PER_WAREHOUSE
								+ " and no_w_id = " + w_id + " limit 1");
				while (neworderResult.next()) {
					no_o_ids.put(neworderResult.getInt(1),
							neworderResult.getInt(2));
				}

				// TODO: requires batch selection.
				// get sum for each d_id in orderlines
				Map<Integer, Double> sums = new HashMap<Integer, Double>();
				for (int d_id : no_o_ids.keySet()) {
					ResultSet olamountResult = _statement
							.executeQuery("select sum(ol_amount) from orderlines where ol_o_id = "
									+ no_o_ids.get(d_id)
									+ " and ol_d_id = "
									+ d_id + " and ol_w_id = " + w_id);
					olamountResult.next();
					sums.put(d_id, olamountResult.getDouble(1));
				}

				// TODO: requires batch selection.
				// get customer_ids in orders
				Map<Integer, Integer> c_ids = new HashMap<Integer, Integer>();
				for (int d_id : no_o_ids.keySet()) {
					// getCId: no_o_id, d_id, w_id
					ResultSet orderResult = _statement
							.executeQuery("select o_c_id from orders where o_id = "
									+ no_o_ids.get(d_id)
									+ " and o_d_id = "
									+ d_id + " and o_w_id = " + w_id);
					orderResult.next();
					c_ids.put(d_id, orderResult.getInt(1));
					System.out.println(c_ids.get(d_id));
				}

				for (int d_id : no_o_ids.keySet()) {
					// deleteNewOrder : d_id, w_id, no_o_id
					_statement
							.addBatch("delete from neworders where no_d_id = "
									+ d_id + " and no_w_id = " + w_id
									+ " and no_o_id = " + no_o_ids.get(d_id));
				}
				// _statement.executeBatch();

				// update ol_delivery_d in orderlines
				for (int d_id : no_o_ids.keySet()) {
					_statement
							.addBatch("update orderlines set ol_delivery_d = "
									+ ol_delivery_d + " where ol_o_id = "
									+ no_o_ids.get(d_id) + " and ol_d_id = "
									+ d_id + " and ol_w_id = " + w_id);
				}
				// _statement.executeBatch();

				// update o_carrier_id in orders
				for (int d_id : no_o_ids.keySet()) {
					// updateOrders : o_carrier_id, no_o_id, d_id, w_id
					_statement.addBatch("update orders set o_carrier_id = "
							+ o_carrier_id + " where o_id = "
							+ no_o_ids.get(d_id) + " and o_d_id = " + d_id
							+ " and o_w_id = " + w_id);
				}
				// _statement.executeBatch();

				// updateCustomer : ol_total, c_id, d_id, w_id
				for (int d_id : no_o_ids.keySet()) {
					_statement
							.addBatch("update customers set c_balance = c_balance + "
									+ sums.get(d_id)
									+ " where c_id = "
									+ c_ids.get(d_id)
									+ " and c_d_id = "
									+ d_id
									+ " and c_w_id = " + w_id);
				}
				_statement.executeBatch();

				for (int d_id : no_o_ids.keySet()) {
					String result = String
							.format("delivery result: district_id=%d, order_id=%d, top_up=%f",
									d_id, no_o_ids.get(d_id), sums.get(d_id));
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
				int ol_cnt = i_ids.size();
				int o_carrier_id = BenchmarkConstant.NULL_CARRIER_ID;

				// getWarehouseTaxRate
				// warehouses -> input
				ResultSet wTaxResult = _statement
						.executeQuery("select w_tax from warehouses where w_id = "
								+ w_id);
				wTaxResult.next();
				double w_tax = wTaxResult.getDouble(1);

				// getDistrict : d_id, w_id
				// districts -> input
				ResultSet dTaxResult = _statement
						.executeQuery("select d_tax, d_next_o_id from districts where d_id = "
								+ d_id + " and d_w_id = " + w_id);
				dTaxResult.next();
				double d_tax = dTaxResult.getDouble(1);
				int d_next_o_id = dTaxResult.getInt(2);
				// incrementNextOrderId : d_next_o_id + 1, d_id, w_id
				_statement.executeUpdate("update districts set d_next_o_id = "
						+ (d_next_o_id + 1) + " where d_id = " + d_id
						+ " and d_w_id = " + w_id);

				// getCustomer : w_id, d_id, c_id
				// customers -> input
				ResultSet customerResult = _statement
						.executeQuery("select c_discount, c_last, c_credit from customers where c_w_id = "
								+ w_id
								+ " and c_d_id = "
								+ d_id
								+ " and c_id= " + c_id);
				customerResult.next();
				double c_discount = customerResult.getDouble(1);

				_ordersInsertion.setInt(1, d_next_o_id);
				_ordersInsertion.setInt(2, c_id);
				_ordersInsertion.setInt(3, d_id);
				_ordersInsertion.setInt(4, w_id);
				_ordersInsertion.setLong(5, o_entry_d);
				_ordersInsertion.setInt(6, o_carrier_id);
				_ordersInsertion.setDouble(7, ol_cnt);
				_ordersInsertion.setBoolean(8, all_local);
				_ordersInsertion.executeUpdate();

				// createNewOrder : d_next_o_id, d_id, w_id
				_newordersInsertion.setInt(1, d_id);
				_newordersInsertion.setInt(2, w_id);
				_newordersInsertion.setInt(3, d_next_o_id);
				_newordersInsertion.executeUpdate();

				// TODO: requires batch selection.
				List<InnerStockState> innerStocks = new LinkedList<InnerStockState>();
				for (int i = 0; i < i_ids.size(); ++i) {
					int ol_supply_w_id = i_w_ids.get(i);
					int ol_i_id = i_ids.get(i);
					int ol_quantity = i_qtys.get(i);
					// getStockInfo : ol_i_id, ol_supply_w_id
					// stocks -> input
					String d_id_str = String.format("%02d", d_id);
					// getStockInfo : ol_i_id, ol_supply_w_id
					ResultSet stockResult = _statement
							.executeQuery("select s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_"
									+ d_id_str
									+ " from stocks where s_i_id = "
									+ ol_i_id
									+ " and s_w_id = "
									+ ol_supply_w_id);
					stockResult.next();
					int s_quantity = stockResult.getInt(1);
					int s_ytd = stockResult.getInt(2);
					int s_order_cnt = stockResult.getInt(3);
					int s_remote_cnt = stockResult.getInt(4);
					String s_dist = stockResult.getString(5);
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
					innerStocks.add(new InnerStockState(s_quantity, s_ytd,
							s_order_cnt, s_remote_cnt, s_dist));
				}
				for (int i = 0; i < i_ids.size(); ++i) {
					int ol_supply_w_id = i_w_ids.get(i);
					int ol_i_id = i_ids.get(i);
					InnerStockState innerStock = innerStocks.get(i);
					_statement.executeUpdate("update stocks set s_quantity = "
							+ innerStock.s_quantity + ", s_ytd = " + innerStock.s_ytd
							+ ", s_order_cnt = " + innerStock.s_order_cnt
							+ ", s_remote_cnt = " + innerStock.s_remote_cnt
							+ " where s_i_id = " + ol_i_id + " and s_w_id = "
							+ ol_supply_w_id);
				}

				double total = 0;
				List<Double> ol_amounts = new LinkedList<Double>();
				for (int i = 0; i < i_ids.size(); ++i) {
					int ol_quantity = i_qtys.get(i);
					ResultSet itemResult = _statement
							.executeQuery("select i_price from items where i_id = "
									+ i_ids.get(i));
					itemResult.next();
					double ol_amount = ol_quantity * itemResult.getDouble(1);
					ol_amounts.add(ol_amount);
					total += ol_amount;
				}
				total *= (1 - c_discount) * (1 + w_tax + d_tax);

				for (int i = 0; i < i_ids.size(); ++i) {
					int ol_number = i + 1;
					int ol_i_id = i_ids.get(i);
					int ol_supply_w_id = i_w_ids.get(i);
					int ol_quantity = i_qtys.get(i);
					// create new order line
					_orderlinesInsertion.setInt(1, d_next_o_id);
					_orderlinesInsertion.setInt(2, d_id);
					_orderlinesInsertion.setInt(3, w_id);
					_orderlinesInsertion.setInt(4, ol_number);
					_orderlinesInsertion.setInt(5, ol_i_id);
					_orderlinesInsertion.setInt(6, ol_supply_w_id);
					_orderlinesInsertion.setLong(7, o_entry_d);
					_orderlinesInsertion.setInt(8, ol_quantity);
					_orderlinesInsertion.setDouble(9, ol_amounts.get(i));
					_orderlinesInsertion.setString(10, innerStocks.get(i).s_dist);
					_orderlinesInsertion.addBatch();
				}
				_orderlinesInsertion.executeBatch();

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
				InnerCustomerState customerState = null;
				if (c_id != -1) {
					ResultSet customerResult = _statement
							.executeQuery("select c_id, c_balance from customers where c_w_id = "
									+ w_id
									+ " and c_d_id = "
									+ d_id
									+ " and c_id = " + c_id);
					customerResult.next();
					customerState = new InnerCustomerState(
							customerResult.getInt(1),
							customerResult.getDouble(2));
				} else {
					// Get the midpoint customer's id
					ResultSet customerResult = _statement
							.executeQuery("select c_id, c_balance from customers where c_w_id = "
									+ w_id
									+ " and c_d_id = "
									+ d_id
									+ " and c_last = '" + fields[3] + "'");
					List<InnerCustomerState> customerList = new LinkedList<InnerCustomerState>();
					while (customerResult.next()) {
						customerList.add(new InnerCustomerState(customerResult
								.getInt(1), customerResult.getDouble(2)));
					}
					customerState = customerList
							.get((customerList.size() - 1) / 2);
					c_id = customerState.c_id;
				}

				// getLastOrder : w_id, d_id, c_id
				ResultSet lastorderResult = _statement
						.executeQuery("select o_id from orders where o_w_id = "
								+ w_id + " and o_d_id = " + d_id
								+ " and o_c_id = " + customerState.c_id
								+ " limit 1");
				if (lastorderResult.next()) {
					_collector.emit(new Values("order_status result: null"));
				} else {
					// getOrderLines : w_id, d_id, order[0]
					int lastorder_id = lastorderResult.getInt(1);
					ResultSet orderlineResult = _statement
							.executeQuery("select ol_i_id from orderlines where ol_w_id = "
									+ w_id
									+ " and ol_d_id = "
									+ d_id
									+ " and ol_o_id = " + lastorder_id);

					while (orderlineResult.next()) {
						String result = String
								.format("order_status result: customer_id=%d, last_order_id=%d, item_id=%d, balance=%f",
										customerState.c_id, lastorder_id,
										orderlineResult.getInt(1),
										customerState.c_balance);
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
				InnerCustomerState customerState = null;
				if (c_id != -1) {
					ResultSet customerResult = _statement
							.executeQuery("select c_id, c_balance from customers where c_w_id = "
									+ w_id
									+ " and c_d_id = "
									+ d_id
									+ " and c_id = " + c_id);
					customerResult.next();
					customerState = new InnerCustomerState(
							customerResult.getInt(1),
							customerResult.getDouble(2));
				} else {
					// Get the midpoint customer's id
					ResultSet customerResult = _statement
							.executeQuery("select c_id, c_balance, c_ytd_payment, c_payment_cnt from customers where c_w_id = "
									+ w_id
									+ " and c_d_id = "
									+ d_id
									+ " and c_last = '" + c_last + "'");
					List<InnerCustomerState> customerList = new LinkedList<InnerCustomerState>();
					while (customerResult.next()) {
						customerList.add(new InnerCustomerState(customerResult
								.getInt(1), customerResult.getDouble(2),
								customerResult.getDouble(3), customerResult
										.getInt(4)));
					}
					customerState = customerList
							.get((customerList.size() - 1) / 2);
					c_id = customerState.c_id;
				}

				double c_balance = customerState.c_balance - h_amount;
				double c_ytd_payment = customerState.c_ytd_payment + h_amount;
				int c_payment_cnt = customerState.c_payment_cnt + 1;

				// getWarehouse
				ResultSet warehouseResult = _statement
						.executeQuery("select w_name from warehouses where w_id="
								+ w_id);
				warehouseResult.next();
				String w_name = warehouseResult.getString(1);

				// getDistrict
				ResultSet districtResult = _statement
						.executeQuery("select d_name from districts where d_w_id="
								+ w_id + " and d_id=" + d_id);
				districtResult.next();
				String d_name = districtResult.getString(1);

				_statement.addBatch("update warehouses set w_ytd = w_ytd + "
						+ h_amount + " where w_id = " + w_id);
				_statement.addBatch("update districts set d_ytd = d_ytd + "
						+ h_amount + " where d_w_id =" + w_id + " and d_id ="
						+ d_id);
				_statement.addBatch("update customers set c_balance = "
						+ c_balance + ", c_ytd_payment = " + c_ytd_payment
						+ ", c_payment_cnt = " + c_payment_cnt
						+ " where c_w_id = " + w_id + " and c_d_id = " + d_id
						+ " and c_id = " + c_id);
				_statement.executeBatch();

				String h_data = BenchmarkRandom.getAstring(
						BenchmarkConstant.MIN_DATA, BenchmarkConstant.MAX_DATA);
				// InsertHistory
				_historiesInsertion.setInt(1, customerState.c_id);
				_historiesInsertion.setInt(2, c_d_id);
				_historiesInsertion.setInt(3, c_w_id);
				_historiesInsertion.setInt(4, d_id);
				_historiesInsertion.setInt(5, w_id);
				_historiesInsertion.setLong(6, h_date);
				_historiesInsertion.setDouble(7, h_amount);
				_historiesInsertion.setString(8, h_data);
				_historiesInsertion.executeUpdate();

				String result = String
						.format("payment result: warehouse_id=%d, district_id=%d, customer_id=%d, balance=%f, ytd_payment=%f, warehouse_name=%s, district_name=%s",
								w_id, d_id, customerState.c_id,
								customerState.c_balance,
								customerState.c_ytd_payment, w_name, d_name);
				_collector.emit(new Values(result));
			}

			// STOCK_LEVEL
			else if (streamname == "STOCK_LEVEL") {
				int w_id = Integer.valueOf(fields[0]);
				int d_id = Integer.valueOf(fields[1]);
				int threshold = Integer.valueOf(fields[2]);
				// getOId
				ResultSet oidResult = _statement
						.executeQuery("select d_next_o_id from districts where d_w_id ="
								+ w_id + " and d_id=" + d_id);
				oidResult.next();
				int next_o_id = oidResult.getInt(1);

				// getStockCount : w_id, d_id, o_id, (o_id-20), w_id, threshold
				ResultSet stockResult = _statement
						.executeQuery("select ol_i_id, s_quantity from orderlines, stocks where ol_w_id = "
								+ w_id
								+ " and ol_d_id = "
								+ d_id
								+ " and ol_o_id < "
								+ next_o_id
								+ " and ol_o_id >= "
								+ (next_o_id - 20)
								+ " and s_w_id = "
								+ w_id
								+ " and s_i_id = ol_i_id and s_quantity < "
								+ threshold);
				while (stockResult.next()) {
					int ol_i_id = stockResult.getInt(1);
					int quantity = stockResult.getInt(2);
					String result = String
							.format("stock_level result: item_id=%d, warehouse_id=%d, quantity=%d",
									ol_i_id, w_id, quantity);
					_collector.emit(new Values(result));
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

				} else if (_numEventCount % 10000 == 0) {
					_numEventCount = 0;
					MemoryReport.reportStatus();
					long elapsedTime = System.currentTimeMillis() - _beginTime;
					System.out.println("query database elapsed time = "
							+ elapsedTime + "ms");

					System.out.println("===================================");
					ResultSet result = null;
					result = _statement
							.executeQuery("select count(*) from items");
					result.next();
					System.out.println("item size=" + result.getInt(1));
					result = _statement
							.executeQuery("select count(*) from warehouses");
					result.next();
					System.out.println("warehouse size=" + result.getInt(1));
					result = _statement
							.executeQuery("select count(*) from districts");
					result.next();
					System.out.println("district size=" + result.getInt(1));
					result = _statement
							.executeQuery("select count(*) from customers");
					result.next();
					System.out.println("customer size=" + result.getInt(1));
					result = _statement
							.executeQuery("select count(*) from orders");
					result.next();
					System.out.println("order size=" + result.getInt(1));
					result = _statement
							.executeQuery("select count(*) from neworders");
					result.next();
					System.out.println("neworder size=" + result.getInt(1));
					result = _statement
							.executeQuery("select count(*) from orderlines");
					result.next();
					System.out.println("orderline size=" + result.getInt(1));
					result = _statement
							.executeQuery("select count(*) from histories");
					result.next();
					System.out.println("history size=" + result.getInt(1));
					result = _statement
							.executeQuery("select count(*) from stocks");
					result.next();
					System.out.println("stock size=" + result.getInt(1));
					System.out.println("===================================");
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
		databaseInit();
		_beginTime = System.currentTimeMillis();
	}

	protected void databaseInit() {
		try {
			_connection = DriverManager.getConnection("jdbc:sqlite::memory:");
			_statement = _connection.createStatement();
			_statement
					.executeUpdate("create table warehouses"
							+ "(w_id smallint, w_name varchar(16), "
							+ "w_street_1 varchar(32), w_street_2 varchar(32), w_city varchar(32), w_state varchar(2), w_zip varchar(9), "
							+ "w_tax float, w_ytd float)");
			_statement
					.executeUpdate("create index warehousesindex on warehouses(w_id)");
			_statement
					.executeUpdate("create table districts"
							+ "(d_id smallint, d_w_id smallint, d_name varchar(16), "
							+ "d_street_1 varchar(32), d_street_2 varchar(32), d_city varchar(32), d_state varchar(2), d_zip varchar(9), "
							+ "d_tax float, d_ytd float, d_next_o_id int)");
			_statement
					.executeUpdate("create index districtsindex on districts(d_w_id, d_id)");
			_statement
					.executeUpdate("create table items"
							+ "(i_id int, i_im_id int, i_name varchar(32), i_price float, i_data varchar(64))");
			_statement.executeUpdate("create index itemsindex on items(i_id)");
			_statement
					.executeUpdate("create table customers"
							+ "(c_id int, c_d_id smallint, c_w_id smallint, "
							+ "c_first varchar(32), c_middle varchar(2), c_last varchar(32), "
							+ "c_street_1 varchar(32), c_street_2 varchar(32), c_city varchar(32), c_state varchar(2), c_zip varchar(9), "
							+ "c_phone varchar(32), c_since bigint, "
							+ "c_credit varchar(2), c_credit_lim float, "
							+ "c_discount float, c_balance float, c_ytd_payment float, c_payment_cnt int, c_delivery_cnt int, "
							+ "c_data varchar(500))");
			_statement
					.executeUpdate("create index customersindex on customers(c_w_id, c_d_id, c_id)");
			_statement
					.executeUpdate("create table orders"
							+ "(o_id int, o_c_id int, o_d_id smallint, o_w_id smallint, o_entry_d bigint, o_carrier_id int, o_ol_cnt int, o_all_local int)");
			_statement
					.executeUpdate("create index ordersindex on orders(o_w_id, o_d_id, o_id)");
			_statement.executeUpdate("create table neworders"
					+ "(no_o_id int, no_d_id smallint, no_w_id smallint)");
			_statement
					.executeUpdate("create index newordersindex on neworders(no_d_id, no_w_id, no_o_id)");
			_statement
					.executeUpdate("create table orderlines"
							+ "(ol_o_id int, ol_d_id smallint, ol_w_id smallint, "
							+ "ol_number int, ol_i_id int, ol_supply_w_id smallint, ol_delivery_d bigint, ol_quantity int, ol_amount float, ol_dist_info varchar(32))");
			_statement
					.executeUpdate("create index orderlinesindex on orderlines(ol_w_id, ol_d_id, ol_o_id, ol_number)");
			_statement
					.executeUpdate("create table histories"
							+ "(h_c_id int, h_c_d_id smallint, h_c_w_id smallint, h_d_id smallint, h_w_id smallint, h_date bigint, h_amount float, h_data varchar(32))");
			_statement
					.executeUpdate("create index historiesindex on histories(h_d_id, h_w_id)");
			_statement
					.executeUpdate("create table stocks"
							+ "(s_i_id int, s_w_id smallint, s_quantity int, "
							+ "s_dist_01 varchar(32), s_dist_02 varchar(32), s_dist_03 varchar(32), s_dist_04 varchar(32), s_dist_05 varchar(32), "
							+ "s_dist_06 varchar(32), s_dist_07 varchar(32), s_dist_08 varchar(32), s_dist_09 varchar(32), s_dist_10 varchar(32), "
							+ "s_ytd int, s_order_cnt int, s_remote_cnt int, s_data varchar(64))");
			_statement
					.executeUpdate("create index stocksindex on stocks(s_w_id, s_i_id)");

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
