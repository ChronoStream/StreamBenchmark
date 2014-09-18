package stream.benchmark.nexmark.query1;

import stream.benchmark.nexmark.spout.NexmarkSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class DriverMain {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new NexmarkSpout());
		builder.setBolt("query", new ConversionBolt()).globalGrouping("spout", "bid");
		builder.setBolt("sink", new SinkBolt()).globalGrouping("query");
		
		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("nexmark", conf, builder.createTopology());
	}
}
