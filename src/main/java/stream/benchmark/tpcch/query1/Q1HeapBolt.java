package stream.benchmark.tpcch.query1;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.tpcch.query1.Q1State.OrderLineState;
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

	private Map<Integer, Map<Integer, List<OrderLineState>>> _orderlineTree;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname == "orderline") {
			// pay attention here!
			int ol_w_id = Integer.valueOf(fields[0]);
			int ol_d_id = Integer.valueOf(fields[1]);
			int ol_quantity = Integer.valueOf(fields[2]);
			double ol_amount = Double.valueOf(fields[3]);
			long ol_delivery_d = Long.valueOf(fields[4]);
			if (!_orderlineTree.containsKey(ol_w_id)) {
				_orderlineTree.put(ol_w_id,
						new HashMap<Integer, List<OrderLineState>>());
			}
			if (!_orderlineTree.containsKey(ol_d_id)) {
				_orderlineTree.get(ol_w_id).put(ol_d_id,
						new LinkedList<OrderLineState>());
			}
			_orderlineTree
					.get(ol_w_id)
					.get(ol_d_id)
					.add(new OrderLineState(ol_quantity, ol_amount,
							ol_delivery_d));
		} else if (streamname == "?") {

		} else if (streamname == "trigger") {
			long time = Integer.valueOf(fields[0]);
			for (Integer warehouse : _orderlineTree.keySet()) {
				for (Integer district : _orderlineTree.get(warehouse).keySet()) {
					int quantity_num = 0;
					int quantity_sum = 0;
					int amount_num = 0;
					int amount_sum = 0;
					for (OrderLineState state : _orderlineTree.get(warehouse)
							.get(district)) {
						if (time - state._ol_delivery_d < 100) {
							quantity_num += 1;
							quantity_sum += state._ol_quantity;
							amount_num += 1;
							amount_sum += state._ol_amount;
							_collector.emit(new Values(warehouse, district,
									quantity_sum, quantity_sum / quantity_num,
									amount_sum, amount_sum / amount_num));
						}
					}
				}
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
		_orderlineTree = new HashMap<Integer, Map<Integer, List<OrderLineState>>>();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("warehouse", "district", "sum_qty",
				"sum_amount", "avg_qty", "avg_amount", "count_order"));
	}

}
