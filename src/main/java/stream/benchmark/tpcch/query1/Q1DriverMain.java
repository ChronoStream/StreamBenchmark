package stream.benchmark.tpcch.query1;

import stream.benchmark.tpcch.query.simple.SimpleSink;
import stream.benchmark.tpcch.spout.TpcchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class Q1DriverMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TpcchSpout());
		BoltDeclarer query = builder.setBolt("query", new Q1HeapBolt());
		query.globalGrouping("spout", "orderline");
		query.globalGrouping("spout", "trigger");
		builder.setBolt("sink", new SimpleSink()).globalGrouping("query");

		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("tpcch", conf, builder.createTopology());
	}

}
