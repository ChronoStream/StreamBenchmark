package stream.benchmark.tpcch.query.simple;

import stream.benchmark.tpcc.spout.TpccSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class SimpleDriverMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TpccSpout());
		BoltDeclarer sink = builder.setBolt("sink", new SimpleSink());
		
		sink.globalGrouping("spout", "item");
		sink.globalGrouping("spout", "warehouse");
		sink.globalGrouping("spout", "district");
		sink.globalGrouping("spout", "customer");
		sink.globalGrouping("spout", "stock");
		sink.globalGrouping("spout", "order");
		sink.globalGrouping("spout", "neworder");
		sink.globalGrouping("spout", "orderline");
		sink.globalGrouping("spout", "history");
		
		sink.globalGrouping("spout", "DELIVERY");
		sink.globalGrouping("spout", "NEW_ORDER");
		sink.globalGrouping("spout", "ORDER_STATUS");
		sink.globalGrouping("spout", "PAYMENT");
		sink.globalGrouping("spout", "STOCK_LEVEL");

		sink.globalGrouping("spout", "trigger");

		Config conf=new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("tpch", conf, builder.createTopology());
	}

}
