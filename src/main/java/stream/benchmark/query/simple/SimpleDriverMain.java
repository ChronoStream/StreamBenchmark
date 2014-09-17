package stream.benchmark.query.simple;

import stream.benchmark.query.SimpleSink;
import stream.benchmark.spout.NexmarkSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class SimpleDriverMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new NexmarkSpout());
		BoltDeclarer sink=builder.setBolt("sink", new SimpleSink());
		sink.globalGrouping("spout", "person");
		sink.globalGrouping("spout", "auction");
		sink.globalGrouping("spout", "bid");
		Config conf=new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("nexmark", conf, builder.createTopology());
	}

}
