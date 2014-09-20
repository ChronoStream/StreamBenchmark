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

	private ScaleParams _scaleParams;
	private Set<Integer> _originalRows;
	private Set<Integer> _selectedRows;

	public TpccSpout() {
		_scaleParams = new ScaleParams(4, 1);
	}

	public TpccSpout(int warehouses, double scalefactor) {
		_scaleParams = new ScaleParams(warehouses, scalefactor);
	}

	public void nextTuple() {

		// if (_numEmittedItems < _scaleParams._numItems) {
		if (_numEmittedItems < 10) {
			// generate items here
			int item_id = _numEmittedItems + 1;
			boolean isOrigin = _originalRows.contains(item_id);
			String tuple = BenchmarkLoader.generateItem(item_id, isOrigin);
			_collector.emit("item", new Values(tuple));
			++_numEmittedItems;
		}

		else if (_numEmittedWarehouses < _scaleParams._warehouses) {
			int w_id = _numEmittedWarehouses + 1;
			// generate warehouses here
			if (_numEmittedDistricts == 0 && _numEmittedCustomers == 0
					&& _numEmittedOrders == 0) {
				String tuple = BenchmarkLoader.generateWarehouse(w_id);
				_collector.emit("warehouse", new Values(tuple));
			}
			if (_numEmittedDistricts < _scaleParams._numDistrictsPerWarehouse) {
				int d_id = _numEmittedDistricts + 1;
				_selectedRows = BenchmarkRandom.selectUniqueIds(
						_scaleParams._numCustomersPerDistrict / 10, 1,
						_scaleParams._numCustomersPerDistrict);
				// generate district here
				if (_numEmittedCustomers == 0 && _numEmittedOrders == 0) {
					int d_next_o_id = _scaleParams._numCustomersPerDistrict;
					String tuple = BenchmarkLoader.generateDistrict(w_id, d_id,
							d_next_o_id);
					_collector.emit("district", new Values(tuple));
				}
				if (_numEmittedCustomers < _scaleParams._numCustomersPerDistrict) {
					// generate customer here
					++_numEmittedCustomers;
					return;
				} else if (_numEmittedOrders < _scaleParams._numCustomersPerDistrict) {
					// generate order here
					++_numEmittedOrders;
					return;
				} else {
					_numEmittedCustomers = 0;
					_numEmittedOrders = 0;
					++_numEmittedDistricts;
					if(_numEmittedDistricts == _scaleParams._numDistrictsPerWarehouse){
						++_numEmittedWarehouses;
					}
				}
			} else {
				_numEmittedDistricts = 0;
			}
		} else {
			// generate execution here
		}
	}

	public void open(Map arg0, TopologyContext arg1,
			SpoutOutputCollector collector) {
		_collector = collector;
		_originalRows = BenchmarkRandom.selectUniqueIds(_scaleParams._numItems,
				1, _scaleParams._numItems);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("item", new Fields("tuple"));
		declarer.declareStream("warehouse", new Fields("tuple"));
		declarer.declareStream("district", new Fields("tuple"));
		declarer.declareStream("customer", new Fields("tuple"));
		declarer.declareStream("stock", new Fields("tuple"));
		declarer.declareStream("orders", new Fields("tuple"));
		declarer.declareStream("new_order", new Fields("tuple"));
		declarer.declareStream("order_line", new Fields("tuple"));
		declarer.declareStream("history", new Fields("tuple"));
		declarer.declareStream("trigger", new Fields("tuple"));
	}

}
