package stream.benchmark.tpcch.query1;

public class Q1State {

	// ol_o_id int, ol_d_id smallint, ol_w_id smallint, 
	// ol_number int, ol_i_id int, ol_supply_w_id smallint, ol_delivery_d bigint, ol_quantity int, ol_amount float, ol_dist_info varchar(32)
	protected static class OrderLineState {
		public OrderLineState(int _ol_o_id, int _ol_d_id, int _ol_w_id,
				int _ol_number, int _ol_i_id, int _ol_supply_w_id,
				long _ol_delivery_d, int _ol_quantity, double _ol_amount,
				String _ol_dist_info) {
			this._ol_o_id = _ol_o_id;
			this._ol_d_id = _ol_d_id;
			this._ol_w_id = _ol_w_id;
			this._ol_number = _ol_number;
			this._ol_i_id = _ol_i_id;
			this._ol_supply_w_id = _ol_supply_w_id;
			this._ol_delivery_d = _ol_delivery_d;
			this._ol_quantity = _ol_quantity;
			this._ol_amount = _ol_amount;
			this._ol_dist_info = _ol_dist_info;
		}

		int _ol_o_id;
		int _ol_d_id;
		int _ol_w_id;
		int _ol_number;
		int _ol_i_id;
		int _ol_supply_w_id;
		long _ol_delivery_d;
		int _ol_quantity;
		double _ol_amount;
		String _ol_dist_info;
	}


}
