package stream.benchmark.nexmark.query3;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SuggestionSink extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	int lastEmitCount=0;
	int count=0;

	public void execute(Tuple input) {
		String person_id = input.getString(0);
		String auction_id = input.getString(1);
		String city = input.getString(2);
		String state = input.getString(3);
		String country = input.getString(4);
		int category = input.getInteger(5);
		int emitCount = input.getInteger(6);
		if(lastEmitCount==emitCount){
			count+=1;
		}else{
			System.out.println("emitCount="+emitCount+", number="+count);
			lastEmitCount=emitCount;
			count=0;
		}
		
//		System.out.println("person_id=" + person_id + ", auction_id="
//				+ auction_id + ", state=" + state + ", emitCount=" + emitCount);
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

}
