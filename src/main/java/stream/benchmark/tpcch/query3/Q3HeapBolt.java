package stream.benchmark.tpcch.query3;

import java.util.List;
import java.util.Map;

import stream.benchmark.tpcch.query3.Q3State.OrderState;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class Q3HeapBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;

	// warehouse -> district -> order -> orderstate
	Map<Integer, Map<Integer, Map<Integer, List<OrderState>>>> orderTree;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname == "neworder") {

		} else if (streamname == "order") {

		} else if (streamname == "orderline") {

		} else if (streamname == "trigger") {
			for (Integer warehouse : orderTree.keySet()) {
				for (Integer district : orderTree.get(warehouse).keySet()) {
					for (Integer order : orderTree.get(warehouse)
							.get(district).keySet()) {
						int ol_amount=0;
						for (OrderState orderstate : orderTree.get(warehouse)
								.get(district).get(order)) {
							
						}
					}
				}
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields());
	}

}
