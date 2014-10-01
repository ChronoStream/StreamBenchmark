package stream.benchmark.tpcc.query;

public class InnerState {
	protected static class InnerCustomerState {
		public InnerCustomerState(int c_id, double c_balance) {
			this.c_id = c_id;
			this.c_balance = c_balance;
		}

		public InnerCustomerState(int c_id, double c_balance,
				double ytd_payment, int c_payment_cnt) {
			this.c_id = c_id;
			this.c_balance = c_balance;
			this.c_ytd_payment = ytd_payment;
			this.c_payment_cnt = c_payment_cnt;
		}

		public int c_id;
		public double c_balance;
		public double c_ytd_payment;
		public int c_payment_cnt;
	}
	
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
