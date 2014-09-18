package stream.benchmark.tpcch.spout;

public class BenchmarkConstant {
	static final int MONEY_DECIMALS=2;
	
	//Item constants
	static final int NUM_ITEMS = 100000;
	static final int MIN_IM = 1;
	static final int MAX_IM = 10000;
	static final double MIN_PRICE = 1.00;
	static final double MAX_PRICE = 100.00;
	static final int MIN_I_NAME = 14;
	static final int MAX_I_NAME = 24;
	static final int MIN_I_DATA = 26;
	static final int MAX_I_DATA = 50;

	//Warehouse constants
	static final double MIN_TAX = 0;
	static final double MAX_TAX = 0.2000;
	static final int TAX_DECIMALS = 4;
	static final double INITIAL_W_YTD = 300000.00;
	static final int MIN_NAME = 6;
	static final int MAX_NAME = 10;
	static final int MIN_STREET = 10;
	static final int MAX_STREET = 20;
	static final int MIN_CITY = 10;
	static final int MAX_CITY = 20;
	static final int STATE = 2;
	static final int ZIP_LENGTH = 9;
	static final String ZIP_SUFFIX = "11111";

	//Stock constants
	static final int MIN_QUANTITY = 10;
	static final int MAX_QUANTITY = 100;
	static final int DIST = 24;
	static final int STOCK_PER_WAREHOUSE = 100000;

	//District constants
	static final int DISTRICTS_PER_WAREHOUSE = 10;
	static final double INITIAL_D_YTD = 30000.00; //different from Warehouse
	static final int INITIAL_NEXT_O_ID = 3001;

	//Customer constants
	static final int CUSTOMERS_PER_DISTRICT = 3000;
	static final double INITIAL_CREDIT_LIM = 50000.00;
	static final double MIN_DISCOUNT = 0.0000;
	static final double MAX_DISCOUNT = 0.5000;
	static final int DISCOUNT_DECIMALS = 4;
	static final double INITIAL_BALANCE = -10.00;
	static final double INITIAL_YTD_PAYMENT = 10.00;
	static final int INITIAL_PAYMENT_CNT = 1;
	static final int INITIAL_DELIVERY_CNT = 0;
	static final int MIN_FIRST = 6;
	static final int MAX_FIRST = 10;
	static final String MIDDLE = "OE";
	static final int PHONE = 16;
	static final int MIN_C_DATA = 300;
	static final int MAX_C_DATA = 500;
	static final String GOOD_CREDIT = "GC";
	static final String BAD_CREDIT = "BC";
}
