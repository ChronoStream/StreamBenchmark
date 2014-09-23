package stream.benchmark.tpcc.spout;

import java.util.Map;
import java.util.Set;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TpccSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector _collector;

	private int _numEmittedItems = 0;
	private int _numEmittedWarehouses = 0;
	private int _numEmittedDistricts = 0;
	private int _numEmittedCustomers = 0;
	private int _numEmittedOrders = 0;
	private int _numEmittedStocks = 0;

	private ScaleParams _scaleParams;
	private BenchmarkExecutor _executor;
	private Set<Integer> _originalRows;
	private Set<Integer> _creditSelectedRows;
	private Set<Integer> _stockSelectedRows;

	public TpccSpout() {
		_scaleParams = new ScaleParams(4, 1);
//		_scaleParams._numItems = 10;
//		_scaleParams._numCustomersPerDistrict = 5;
//		_scaleParams._numNewOrdersPerDistrict = 2;
	}

	public TpccSpout(int warehouses, double scalefactor) {
		_scaleParams = new ScaleParams(warehouses, scalefactor);
	}

	public void nextTuple() {
		// generate items here, correct
		if (_numEmittedItems < _scaleParams._numItems) {
			if (_numEmittedItems == 0) {
				_originalRows = BenchmarkRandom.selectUniqueIds(
						_scaleParams._numItems, 1, _scaleParams._numItems);
			}
			int item_id = _numEmittedItems + 1;
			boolean isOrigin = _originalRows.contains(item_id);
			String tuple = BenchmarkLoader.generateItem(item_id, isOrigin);
			// item_id, item_image_id, item_name, item_price, item_data
			_collector.emit("item", new Values(tuple));
			++_numEmittedItems;
			return;
		}

		// generate warehouses, districts, customers, orders, stocks here
		if (_numEmittedWarehouses < _scaleParams._warehouses) {
			int w_id = _numEmittedWarehouses + 1;
			// generate warehouses here, correct
			if (_numEmittedDistricts == 0 && _numEmittedCustomers == 0
					&& _numEmittedOrders == 0) {
				String warehouseTuple = BenchmarkLoader.generateWarehouse(w_id);
				// warehouse_id, warehouse_name, warehouse_address,
				// warehouse_tax, warehouse_ytd
				_collector.emit("warehouse", new Values(warehouseTuple));
			}
			// generate districts, customers, orders here
			if (_numEmittedDistricts < _scaleParams._numDistrictsPerWarehouse) {
				int d_id = _numEmittedDistricts + 1;
				// generate district here
				if (_numEmittedCustomers == 0 && _numEmittedOrders == 0) {
					_creditSelectedRows = BenchmarkRandom.selectUniqueIds(
							_scaleParams._numCustomersPerDistrict / 10, 1,
							_scaleParams._numCustomersPerDistrict);
					int d_next_o_id = _scaleParams._numCustomersPerDistrict + 1;

					String districtTuple = BenchmarkLoader.generateDistrict(
							w_id, d_id, d_next_o_id);
					// district_id, warehouse_id, district_name,
					// district_address, district_tax, district_ytd
					_collector.emit("district", new Values(districtTuple));
				}
				// generate customer here
				if (_numEmittedCustomers < _scaleParams._numCustomersPerDistrict) {
					int c_id = _numEmittedCustomers + 1;
					boolean badCredit = _creditSelectedRows.contains(c_id);
					String customerTuple = BenchmarkLoader.generateCustomer(
							w_id, d_id, c_id, badCredit);
					// customer_id, district, warehouse, name, address, phone,
					// create_time, credit, credit_limit, discount, balance,
					// tyd_payment, payment_count, delivery_count, data
					_collector.emit("customer", new Values(customerTuple));

					String historyTuple = BenchmarkLoader.generateHistory(w_id,
							d_id, c_id);
					_collector.emit("history", new Values(historyTuple));
					++_numEmittedCustomers;
					return;
				}
				// generate order here
				if (_numEmittedOrders < _scaleParams._numCustomersPerDistrict) {
					int o_id = _numEmittedOrders + 1;
					int o_ol_cnt = BenchmarkRandom.getNumber(
							BenchmarkConstant.MIN_OL_CNT,
							BenchmarkConstant.MAX_OL_CNT);
					boolean newOrder = (_scaleParams._numCustomersPerDistrict - _scaleParams._numNewOrdersPerDistrict) < o_id;
					String orderTuple = BenchmarkLoader.generateOrder(w_id,
							d_id, o_id, o_id, o_ol_cnt, newOrder);
					_collector.emit("order", new Values(orderTuple));
					for (int ol_number = 0; ol_number < o_ol_cnt; ++ol_number) {
						String orderlineTuple = BenchmarkLoader.generateOrderLine(
								w_id, d_id, o_id, ol_number,
								_scaleParams._numItems, newOrder);
						_collector
								.emit("orderline", new Values(orderlineTuple));
					}
					if (newOrder) {
						String neworderTuple = o_id + "," + d_id + "," + w_id;
						_collector.emit("neworder", new Values(neworderTuple));
					}
					++_numEmittedOrders;
					return;
				}
				++_numEmittedDistricts;
				_numEmittedCustomers = 0;
				_numEmittedOrders = 0;
				return;
			}

			if (_numEmittedStocks == 0) {
				_stockSelectedRows = BenchmarkRandom.selectUniqueIds(
						_scaleParams._numItems / 10, 1, _scaleParams._numItems);
			}
			if (_numEmittedStocks < _scaleParams._numItems + 1) {
				int i_id = _numEmittedStocks + 1;
				boolean original = _stockSelectedRows.contains(i_id);
				String stockTuple = BenchmarkLoader.generateStock(w_id, i_id,
						original);
				_collector.emit("stock", new Values(stockTuple));
				++_numEmittedStocks;
				return;
			}
			++_numEmittedWarehouses;
			_numEmittedDistricts = 0;
			_numEmittedStocks = 0;
			return;
		}

		// generate execution here
		int x = BenchmarkRandom.getNumber(1, 100);
		if (x <= 4) {
			String param = _executor.generateStockLevelParams();
			_collector.emit("STOCK_LEVEL", new Values(param));
		} else if (x <= 4 + 4) {
			String param = _executor.generateDeliveryParams();
			_collector.emit("DELIVERY", new Values(param));
		} else if (x <= 4 + 4 + 4) {
			String param = _executor.generateOrderStatusParams();
			_collector
					.emit("ORDER_STATUS", new Values(param));
		} else if (x <= 43 + 4 + 4 + 4) {
			String param = _executor.generatePaymentParams();
			_collector.emit("PAYMENT", new Values(param));
		} else {
			String param = _executor.generateNewOrderParams();
			_collector.emit("NEW_ORDER", new Values(param));
		}
	}

	public void open(Map arg0, TopologyContext arg1,
			SpoutOutputCollector collector) {
		_collector = collector;
		_executor = new BenchmarkExecutor(_scaleParams);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("item", new Fields("tuple"));
		declarer.declareStream("warehouse", new Fields("tuple"));
		declarer.declareStream("district", new Fields("tuple"));
		declarer.declareStream("customer", new Fields("tuple"));
		declarer.declareStream("stock", new Fields("tuple"));
		declarer.declareStream("order", new Fields("tuple"));
		declarer.declareStream("neworder", new Fields("tuple"));
		declarer.declareStream("orderline", new Fields("tuple"));
		declarer.declareStream("history", new Fields("tuple"));
		declarer.declareStream("trigger", new Fields("tuple"));
		declarer.declareStream("DELIVERY", new Fields("param"));
		declarer.declareStream("NEW_ORDER", new Fields("param"));
		declarer.declareStream("ORDER_STATUS", new Fields("param"));
		declarer.declareStream("PAYMENT", new Fields("param"));
		declarer.declareStream("STOCK_LEVEL", new Fields("param"));
	}

}
