package stream.benchmark.tpcc.spout;

import java.io.Serializable;

public class ScaleParams implements Serializable {

	private static final long serialVersionUID = 1L;
	public int _warehouses;
	public double _scalefactor;
	public int _starting_warehouse;
	public int _ending_warehouse;
	public int _numItems;
	public int _numDistrictsPerWarehouse;
	public int _numCustomersPerDistrict;
	public int _numNewOrdersPerDistrict;

	public ScaleParams(int warehouses, double scalefactor) {
		_warehouses = warehouses;
		_scalefactor = scalefactor;

		_starting_warehouse = 1;
		_ending_warehouse = _starting_warehouse + warehouses - 1;

		_numItems = (int) (BenchmarkConstant.NUM_ITEMS / scalefactor);
		_numDistrictsPerWarehouse = BenchmarkConstant.DISTRICTS_PER_WAREHOUSE;
		_numCustomersPerDistrict = (int) (BenchmarkConstant.CUSTOMERS_PER_DISTRICT / scalefactor);
		_numNewOrdersPerDistrict = (int) (BenchmarkConstant.INITIAL_NEW_ORDERS_PER_DISTRICT / scalefactor);
	}
}
