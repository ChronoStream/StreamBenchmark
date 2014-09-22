package stream.benchmark.tpcc.query;

public class FetchResult {

	protected static class NewOrderItemInfo {
		public NewOrderItemInfo(double i_price, String i_name, String i_data) {
			this._i_price = i_price;
			this._i_name = i_name;
			this._i_data = i_data;
		}

		double _i_price;
		String _i_name;
		String _i_data;
	}

	protected static class NewOrderItemData {
		public NewOrderItemData(double i_price, String i_name, String i_data) {
			this._i_price = i_price;
			this._i_name = i_name;
			this._i_data = i_data;
		}

		double _i_price;
		String _i_name;
		String _i_data;
	}
	
}
