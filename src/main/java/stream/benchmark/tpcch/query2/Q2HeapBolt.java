package stream.benchmark.tpcch.query2;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.tpcch.query2.Q2State.ItemState;
import stream.benchmark.tpcch.query2.Q2State.NationState;
import stream.benchmark.tpcch.query2.Q2State.StockState;
import stream.benchmark.tpcch.query2.Q2State.SupplierState;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Q2HeapBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;
	
	long _beginTime;

	// s_i_id -> <s_w_id, s_quantity>
	Map<Integer, List<StockState>> _stocks;
	Map<Integer, SupplierState> _suppliers;
	Map<Integer, NationState> _nations;
	Map<Integer, String> _regions;
	Map<Integer, ItemState> _items;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname == "item") {
			_items.put(Integer.valueOf(fields[0]), new ItemState(fields[2],
					Double.valueOf(fields[3])));
		} else if (streamname.equals("supplier")) {
			_suppliers.put(Integer.valueOf(fields[0]), new SupplierState(
					fields[1], Integer.valueOf(fields[3])));
		} else if (streamname.equals("stock")) {
			int item_id = Integer.valueOf(fields[0]);
			int warehouse_id = Integer.valueOf(fields[1]);
			if (!_stocks.containsKey(item_id)) {
				_stocks.put(item_id, new LinkedList<StockState>());
			}
			_stocks.get(item_id).add(
					new StockState(warehouse_id, Integer.valueOf(fields[2])));
		} else if (streamname.equals("region")) {
			_regions.put(Integer.valueOf(fields[0]), fields[1]);
		} else if (streamname.equals("nation")) {
			_nations.put(Integer.valueOf(fields[0]), new NationState(fields[1],
					Integer.valueOf(fields[2])));
		}

		if (streamname == "DELIVERY" || streamname == "NEW_ORDER"
				|| streamname == "ORDER_STATUS" || streamname == "PAYMENT"
				|| streamname == "STOCK_LEVEL") {
			if (System.currentTimeMillis() - _beginTime < 2000) {
				return;
			}
			for (Integer stock_id : _stocks.keySet()){
				int max_quantity = -1;
				int max_supplier_id = -1;
				for (StockState tmpstock : _stocks.get(stock_id)){
					int supplier_key = (tmpstock._w_id * stock_id) % 10000;
					int nation_id = _suppliers.get(supplier_key)._su_nation_id;
					int region_id = _nations.get(nation_id)._region_key;
					
					if (!_regions.get(region_id).equals("Europe")){
						continue;
					}
					if (tmpstock._quantity > max_quantity){
						max_quantity = tmpstock._quantity;
						max_supplier_id = supplier_key;
					}
					_collector.emit(new Values(""));
				}
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;

		_stocks = new HashMap<Integer, List<StockState>>();
		_suppliers = new HashMap<Integer, SupplierState>();
		_nations = new HashMap<Integer, NationState>();
		_regions = new HashMap<Integer, String>();
		_items = new HashMap<Integer, ItemState>();
		
		_beginTime = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
