package stream.benchmark.query3;

import stream.benchmark.spout.NexmarkSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class SuggestionDriverMain {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new NexmarkSpout());
		BoltDeclarer suggestion=builder.setBolt("query", new SuggestionDBBolt());
		suggestion.globalGrouping("spout", "auction");
		suggestion.globalGrouping("spout", "person");
		
		builder.setBolt("sink", new SuggestionSink()).globalGrouping("query");
		
		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("nexmark", conf, builder.createTopology());
	}
}
