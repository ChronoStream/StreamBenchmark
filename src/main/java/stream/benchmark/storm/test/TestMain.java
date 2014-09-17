package stream.benchmark.storm.test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TestMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TestSpout(), 1);
		builder.setBolt("bolt", new TestBolt()).globalGrouping("spout");
		builder.setBolt("sink", new TestSink()).globalGrouping("bolt");
		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("nexmark", conf, builder.createTopology());
	}

}
