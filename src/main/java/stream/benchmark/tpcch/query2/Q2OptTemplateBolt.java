package stream.benchmark.tpcch.query2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.toolkits.MemoryReport;
import stream.benchmark.tpcch.query.TableState.ItemState;
import stream.benchmark.tpcch.query.TableState.NationState;
import stream.benchmark.tpcch.query.TableState.RegionState;
import stream.benchmark.tpcch.query.TableState.StockState;
import stream.benchmark.tpcch.query.TableState.SupplierState;
import stream.benchmark.tpcch.spout.BenchmarkConstant;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Q2OptTemplateBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;

	private Connection _connection = null;
	private Statement _statement = null;
	private PreparedStatement _itemsInsertion;
	private PreparedStatement _stocksInsertion;

	private Map<Integer, ItemState> _itemsIndex;
	private Map<Integer, Map<Integer, StockState>> _stocksIndex;
	private Map<Integer, NationState> _nationsIndex;
	private Map<Integer, RegionState> _regionsIndex;
	private Map<Integer, SupplierState> _suppliersIndex;

	private boolean _isFirstQuery = true;
	private long _beginTime;

	private int _numReadItems = 0;
	private int _numReadStocks = 0;
	private int _numReadNations = 0;
	private int _numReadRegions = 0;
	private int _numReadSuppliers = 0;

	private int _numWriteItems = 0;
	private int _numWriteStocks = 0;
	private int _numWriteNations = 0;
	private int _numWriteRegions = 0;
	private int _numWriteSuppliers = 0;

	private int _numEventCount = 0;

	public void execute(Tuple input) {
		try {
			String tuple = input.getString(0);
			String[] fields = tuple.split(",");
			String streamname = input.getSourceStreamId();
			// item
			if (streamname == "item") {
				int item_id = Integer.valueOf(fields[0]);
				ItemState item = new ItemState(item_id,
						Integer.valueOf(fields[1]), fields[2],
						Double.valueOf(fields[3]), fields[4]);
				_itemsIndex.put(item_id, item);

//				_itemsInsertion.setInt(1, Integer.valueOf(fields[0]));
//				_itemsInsertion.setInt(2, Integer.valueOf(fields[1]));
//				_itemsInsertion.setString(3, fields[2]);
//				_itemsInsertion.setDouble(4, Double.valueOf(fields[3]));
//				_itemsInsertion.setString(5, fields[4]);
//				_itemsInsertion.addBatch();
//				_itemsInsertion.executeUpdate();
			}
			// stock
			else if (streamname == "stock") {
				int item_id = Integer.valueOf(fields[0]);
				int warehouse_id = Integer.valueOf(fields[1]);
				int quantity = Integer.valueOf(fields[2]);
				List<String> dists = new LinkedList<String>();
				int tmpId = 3;
				for (; tmpId < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE + 3; ++tmpId) {
					dists.add(fields[tmpId]);
				}
				int ytd = Integer.valueOf(fields[tmpId++]);
				int order_cnt = Integer.valueOf(fields[tmpId++]);
				int remote_cnt = Integer.valueOf(fields[tmpId++]);
				String data = fields[tmpId++];
				StockState stock = new StockState(item_id, warehouse_id,
						quantity, dists, ytd, order_cnt, remote_cnt, data);
				if (!_stocksIndex.containsKey(item_id)) {
					_stocksIndex.put(item_id,
							new HashMap<Integer, StockState>());
				}
				if (!_stocksIndex.get(item_id).containsKey(warehouse_id)) {
					_stocksIndex.get(item_id).put(warehouse_id, stock);
				}
				
//				_stocksInsertion.setInt(1, Integer.valueOf(fields[0]));
//				_stocksInsertion.setInt(2, Integer.valueOf(fields[1]));
//				_stocksInsertion.setInt(3, Integer.valueOf(fields[2]));
//				int tmpId = 3;
//				for (; tmpId < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE + 3; ++tmpId) {
//					_stocksInsertion.setString(tmpId + 1, fields[tmpId]);
//				}
//				_stocksInsertion.setInt(tmpId + 1,
//						Integer.valueOf(fields[tmpId]));
//				++tmpId;
//				_stocksInsertion.setInt(tmpId + 1,
//						Integer.valueOf(fields[tmpId]));
//				++tmpId;
//				_stocksInsertion.setInt(tmpId + 1,
//						Integer.valueOf(fields[tmpId]));
//				++tmpId;
//				_stocksInsertion.setString(tmpId + 1, fields[tmpId]);
//				_stocksInsertion.addBatch();
//				_stocksInsertion.executeUpdate();
			}
			// nation
			else if (streamname == "nation") {
				int n_id = Integer.valueOf(fields[0]);
				String n_name = fields[1];
				int r_id = Integer.valueOf(fields[2]);
				NationState nation = new NationState(n_id, n_name, r_id);
				_nationsIndex.put(n_id, nation);
			}
			// region
			else if (streamname == "region") {
				int r_id = Integer.valueOf(fields[0]);
				String r_name = fields[1];
				RegionState region = new RegionState(r_id, r_name);
				_regionsIndex.put(r_id, region);
			}
			// supplier
			else if (streamname == "supplier") {
				int su_id = Integer.valueOf(fields[0]);
				String su_name = fields[1];
				String su_address = fields[2];
				int n_id = Integer.valueOf(fields[3]);
				SupplierState supplier = new SupplierState(su_id, su_name,
						su_address, n_id);
				_suppliersIndex.put(su_id, supplier);
			}
			// NEW_ORDER
			else if (streamname == "NEW_ORDER") {
				int w_id = Integer.valueOf(fields[0]);
				List<Integer> i_ids = new LinkedList<Integer>();
				for (String tmp : fields[4].split(";")) {
					i_ids.add(Integer.valueOf(tmp));
				}
				List<Integer> i_w_ids = new LinkedList<Integer>();
				for (String tmp : fields[5].split(";")) {
					i_w_ids.add(Integer.valueOf(tmp));
				}
				List<Integer> i_qtys = new LinkedList<Integer>();
				for (String tmp : fields[6].split(";")) {
					i_qtys.add(Integer.valueOf(tmp));
				}

				for (int i = 0; i < i_ids.size(); ++i) {
					int ol_supply_w_id = i_w_ids.get(i);
					int ol_i_id = i_ids.get(i);
					int ol_quantity = i_qtys.get(i);

					// getStockInfo : ol_i_id, ol_supply_w_id
					StockState stockInfo = _stocksIndex.get(ol_i_id).get(
							ol_supply_w_id);
					int s_quantity = stockInfo._quantity;
					int s_ytd = stockInfo._ytd;
					int s_order_cnt = stockInfo._order_cnt;
					int s_remote_cnt = stockInfo._remote_cnt;
					s_ytd += ol_quantity;
					if (s_quantity >= ol_quantity + 10) {
						s_quantity = s_quantity - ol_quantity;
					} else {
						s_quantity = s_quantity + 91 - ol_quantity;
					}
					s_order_cnt += 1;
					if (ol_supply_w_id != w_id) {
						s_remote_cnt += 1;
					}
					stockInfo._quantity = s_quantity;
					stockInfo._ytd = s_ytd;
					stockInfo._order_cnt = s_order_cnt;
					stockInfo._remote_cnt = s_remote_cnt;
					++_numReadStocks;
					++_numWriteStocks;
				}
				// event count
				++_numEventCount;
			}

			if (streamname == "NEW_ORDER") {
				if (_isFirstQuery) {
					long elapsedTime = System.currentTimeMillis() - _beginTime;
					System.out.println("load database elapsed time = "
							+ elapsedTime + "ms");
					_isFirstQuery = false;
					_beginTime = System.currentTimeMillis();

				} else if (_numEventCount % 200000 == 0) {
					_numEventCount = 0;
					System.out
							.println("################################################");
					System.out.println("elapsed consume time = "
							+ (System.currentTimeMillis() - _beginTime) + "ms");
					// /////////////////////////////////////////////////////////////////
					long startQueryTime = System.currentTimeMillis();
					StringBuilder sb = new StringBuilder();
					int max_quantity = -1;
					int max_item_id = -1;
					// find the item with maximum stock quantity
					for (int s_i_id : _stocksIndex.keySet()) {
						int local_quantity = 0;
						for (int warehouse_id : _stocksIndex.get(s_i_id)
								.keySet()) {
							int supplier_id = (s_i_id * warehouse_id) % 10000;
							int nation_id = _suppliersIndex.get(supplier_id)._n_id;
							String region_name = _regionsIndex
									.get(_nationsIndex.get(nation_id)._r_id)._r_name;
							if (region_name.equals("EUROPE")) {
								local_quantity += _stocksIndex.get(s_i_id).get(
										warehouse_id)._quantity;
							}
						}
						if (local_quantity > max_quantity) {
							max_quantity = local_quantity;
							max_item_id = s_i_id;
						}
					}
					if (max_item_id != -1) {
						for (int warehouse_id : _stocksIndex.get(max_item_id)
								.keySet()) {
							int supplier_id = (max_item_id * warehouse_id) % 10000;
							int nation_id = _suppliersIndex.get(supplier_id)._n_id;
							String region_name = _regionsIndex
									.get(_nationsIndex.get(nation_id)._r_id)._r_name;
							String item_name = _itemsIndex.get(max_item_id)._name;

							++_numReadItems;

							sb.append(max_item_id);
							sb.append(", ");
							sb.append(warehouse_id);
							sb.append(", ");
							sb.append(_nationsIndex.get(nation_id)._n_name);
							sb.append(", ");
							sb.append(region_name);
							sb.append(", ");
							sb.append(max_quantity);
							sb.append(", ");
							sb.append(item_name);
							_collector.emit(new Values(sb.toString()));
							sb.setLength(0);
						}
					}
					System.out.println("elapsed query time = "
							+ (System.currentTimeMillis() - startQueryTime)
							+ "ms");

					MemoryReport.reportStatus();
					System.out.println("*************READ**************");
					System.out.println("read item size=" + _numReadItems);
					System.out.println("read stock size=" + _numReadStocks);
					System.out.println("read nation size=" + _numReadNations);
					System.out.println("read region size=" + _numReadRegions);
					System.out.println("read supplier size="
							+ _numReadSuppliers);
					System.out.println("===================================");

					System.out.println("*************WRITE**************");
					System.out.println("write item size=" + _numWriteItems);
					System.out.println("write stock size=" + _numWriteStocks);
					System.out.println("write nation size=" + _numWriteNations);
					System.out.println("write region size=" + _numWriteRegions);
					System.out.println("write supplier size="
							+ _numWriteSuppliers);
					System.out.println("===================================");
					_numReadItems = 0;
					_numReadStocks = 0;
					_numReadNations = 0;
					_numReadRegions = 0;
					_numReadSuppliers = 0;

					_numWriteItems = 0;
					_numWriteStocks = 0;
					_numWriteNations = 0;
					_numWriteRegions = 0;
					_numWriteSuppliers = 0;

					int count = 0;
					for (Map<Integer, StockState> stockmap : _stocksIndex
							.values()) {
						count += stockmap.size();
					}
					System.out.println("stocks size=" + count);

					_beginTime = System.currentTimeMillis();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;

		_itemsIndex = new HashMap<Integer, ItemState>();
		_stocksIndex = new HashMap<Integer, Map<Integer, StockState>>();
		_nationsIndex = new HashMap<Integer, NationState>();
		_regionsIndex = new HashMap<Integer, RegionState>();
		_suppliersIndex = new HashMap<Integer, SupplierState>();

		databaseInit();

		_beginTime = System.currentTimeMillis();
	}

	protected void databaseInit() {
		try {
			_connection = DriverManager.getConnection("jdbc:sqlite::memory:");
			_statement = _connection.createStatement();
			// items
			_statement
					.executeUpdate("create table items"
							+ "(i_id int, i_im_id int, i_name varchar(32), i_price float, i_data varchar(64))");
			_statement.executeUpdate("create index itemsindex on items(i_id)");
			// stocks
			_statement
					.executeUpdate("create table stocks"
							+ "(s_i_id int, s_w_id smallint, s_quantity int, "
							+ "s_dist_01 varchar(32), s_dist_02 varchar(32), s_dist_03 varchar(32), s_dist_04 varchar(32), s_dist_05 varchar(32), "
							+ "s_dist_06 varchar(32), s_dist_07 varchar(32), s_dist_08 varchar(32), s_dist_09 varchar(32), s_dist_10 varchar(32), "
							+ "s_ytd int, s_order_cnt int, s_remote_cnt int, s_data varchar(64)) "
							+ "engine=memory");
			_statement
					.executeUpdate("create index stocksindex on stocks(s_w_id, s_i_id)");

			_itemsInsertion = _connection
					.prepareStatement("insert into items values(?, ?, ?, ?, ?)");
			_stocksInsertion = _connection
					.prepareStatement("insert into stocks values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result"));
	}
}
