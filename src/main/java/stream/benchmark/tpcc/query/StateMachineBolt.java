package stream.benchmark.tpcc.query;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StateMachineBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;
	
	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if(streamname=="item"){
			
		}else if(streamname=="warehouse"){
			
		}else if(streamname=="district"){
			
		}else if(streamname=="customer"){
			
		}else if(streamname=="stock"){
			
		}else if(streamname=="order"){
			
		}else if(streamname=="neworder"){
			
		}else if(streamname=="orderline"){
			
		}else if(streamname=="history"){
			
		}else if(streamname=="DELIVERY"){
			
		}else if(streamname=="NEW_ORDER"){
			
		}else if(streamname=="ORDER_STATUS"){
			
		}else if(streamname=="PAYMENT"){
			
		}else if(streamname=="STOCK_LEVEL"){
			
		}else{
			
		}
		_collector.emit(new Values(streamname));
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}

}
