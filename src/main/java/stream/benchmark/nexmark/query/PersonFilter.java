package stream.benchmark.nexmark.query;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PersonFilter extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector=collector;
	}

	public void execute(Tuple input) {
		// schema: person_id, street_name, email, city, state, country
		String tuple=input.getString(0);
		String[] fields = tuple.split(",");
		_collector.emit("person", new Values(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("person", new Fields("person_id", "street_name", "email", "city", "state", "country"));
	}

}
