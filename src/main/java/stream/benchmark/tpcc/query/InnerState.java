package stream.benchmark.tpcc.query;

public class InnerState {
	static class InnerCustomerState {
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
}
