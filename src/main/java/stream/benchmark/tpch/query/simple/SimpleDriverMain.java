package stream.benchmark.tpch.query.simple;

import stream.benchmark.tpch.spout.TpchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class SimpleDriverMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TpchSpout());
		builder.setBolt("sink", new SimpleSink()).globalGrouping("spout", "sink");
		Config conf=new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("tpch", conf, builder.createTopology());
	}

}
