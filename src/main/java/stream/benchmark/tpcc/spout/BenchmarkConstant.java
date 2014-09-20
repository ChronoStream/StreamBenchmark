package stream.benchmark.tpcc.spout;

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
	
	//Order constants
	static final int MIN_CARRIER_ID = 1;
	static final int MAX_CARRIER_ID = 10;
	//HACK: This is not strictly correct, but it works
	static final long NULL_CARRIER_ID = 0L;
	//o_id < than this value, carrier != null, >= -> carrier == null
	static final int NULL_CARRIER_LOWER_BOUND = 2101;
	static final int MIN_OL_CNT = 5;
	static final int MAX_OL_CNT = 15;
	static final int INITIAL_ALL_LOCAL = 1;
	static final int INITIAL_ORDERS_PER_DISTRICT = 3000;

	//Used to generate new order transactions
	static final int MAX_OL_QUANTITY = 10;

	//Order line constants
	static final int INITIAL_QUANTITY = 5;
	static final double MIN_AMOUNT = 0.01;

	//History constants
	static final int MIN_DATA = 12;
	static final int MAX_DATA = 24;
	static final double INITIAL_AMOUNT = 10.00;

	//New order constants
	static final int INITIAL_NEW_ORDERS_PER_DISTRICT = 900;

	//TPC-C 2.4.3.4 (page 31) says this must be displayed when new order rolls back.
	static final String INVALID_ITEM_MESSAGE = "Item number is not valid";

	//Used to generate stock level transactions
	static final int MIN_STOCK_LEVEL_THRESHOLD = 10;
	static final int MAX_STOCK_LEVEL_THRESHOLD = 20;

	//Used to generate payment transactions
	static final double MIN_PAYMENT = 1.0;
	static final double MAX_PAYMENT = 5000.0;

	//Indicates "brand" items and stock in i_data and s_data.
	static final String ORIGINAL_STRING = "ORIGINAL";

	//Table Names
	static final String TABLENAME_ITEM       = "ITEM";
	static final String TABLENAME_WAREHOUSE  = "WAREHOUSE";
	static final String TABLENAME_DISTRICT   = "DISTRICT";
	static final String TABLENAME_CUSTOMER   = "CUSTOMER";
	static final String TABLENAME_STOCK      = "STOCK";
	static final String TABLENAME_ORDERS     = "ORDERS";
	static final String TABLENAME_NEW_ORDER  = "NEW_ORDER";
	static final String TABLENAME_ORDER_LINE = "ORDER_LINE";
	static final String TABLENAME_HISTORY    = "HISTORY";
}
