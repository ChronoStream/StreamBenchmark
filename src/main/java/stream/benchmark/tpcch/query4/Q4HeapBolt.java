package stream.benchmark.tpcch.query4;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class Q4HeapBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;
	
	// warehouse -> district -> order -> list<delivery_d>
	Map<Integer, Map<Integer, Map<Integer, List<Double>>>> _orderTree;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if(streamname == "orders"){
			
		} else if (streamname == "orderlines"){
			
		} else if (streamname == ""){
			
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
