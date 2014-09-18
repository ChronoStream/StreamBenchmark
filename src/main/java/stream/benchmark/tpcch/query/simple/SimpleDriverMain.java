package stream.benchmark.tpcch.query.simple;

import stream.benchmark.tpcch.spout.TpcchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class SimpleDriverMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TpcchSpout());
		BoltDeclarer sink = builder.setBolt("sink", new SimpleSink());
		
		sink.globalGrouping("spout", "item");
		sink.globalGrouping("spout", "warehouse");
		sink.globalGrouping("spout", "district");
		sink.globalGrouping("spout", "customer");
		sink.globalGrouping("spout", "stock");
		sink.globalGrouping("spout", "orders");
		sink.globalGrouping("spout", "new_order");
		sink.globalGrouping("spout", "order_line");
		sink.globalGrouping("spout", "history");
		sink.globalGrouping("spout", "trigger");

		Config conf=new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("tpch", conf, builder.createTopology());
	}

}
