package stream.benchmark.tpcch.query;

public class InnerState {

	public static class NewOrderItemInfo {
		public NewOrderItemInfo(double i_price, String i_name, String i_data) {
			this._i_price = i_price;
			this._i_name = i_name;
			this._i_data = i_data;
		}

		public double _i_price;
		public String _i_name;
		public String _i_data;
	}

	public static class NewOrderItemData {
		public NewOrderItemData(String _i_name, int _s_quantity,
				String _brand_generic, double _i_price, double _ol_amount) {
			this._i_name = _i_name;
			this._s_quantity = _s_quantity;
			this._brand_generic = _brand_generic;
			this._i_price = _i_price;
			this._ol_amount = _ol_amount;
		}

		public String _i_name;
		public int _s_quantity;
		public String _brand_generic;
		public double _i_price;
		public double _ol_amount;
	}

}
