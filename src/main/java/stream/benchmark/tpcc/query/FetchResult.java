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
		public NewOrderItemData(String _i_name, int _s_quantity,
				String _brand_generic, double _i_price, double _ol_amount) {
			this._i_name = _i_name;
			this._s_quantity = _s_quantity;
			this._brand_generic = _brand_generic;
			this._i_price = _i_price;
			this._ol_amount = _ol_amount;
		}

		String _i_name;
		int _s_quantity;
		String _brand_generic;
		double _i_price;
		double _ol_amount;
	}

}
