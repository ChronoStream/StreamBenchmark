package stream.benchmark.tpcch.spout;

public class BenchmarkConstant {
	public static final int MONEY_DECIMALS=2;
	
	//Item constants
	public static final int NUM_ITEMS = 1000; // origin : 100000
	public static final int MIN_IM = 1;
	public static final int MAX_IM = 10000;
	public static final double MIN_PRICE = 1.00;
	public static final double MAX_PRICE = 100.00;
	public static final int MIN_I_NAME = 14;
	public static final int MAX_I_NAME = 24;
	public static final int MIN_I_DATA = 26;
	public static final int MAX_I_DATA = 50;

	//Warehouse constants
	public static final double MIN_TAX = 0;
	public static final double MAX_TAX = 0.2000;
	public static final int TAX_DECIMALS = 4;
	public static final double INITIAL_W_YTD = 300000.00;
	public static final int MIN_NAME = 6;
	public static final int MAX_NAME = 10;
	public static final int MIN_STREET = 10;
	public static final int MAX_STREET = 20;
	public static final int MIN_CITY = 10;
	public static final int MAX_CITY = 20;
	public static final int STATE = 2;
	public static final int ZIP_LENGTH = 9;
	public static final String ZIP_SUFFIX = "11111";

	//Stock constants
	public static final int MIN_QUANTITY = 10;
	public static final int MAX_QUANTITY = 100;
	public static final int DIST = 24;
	public static final int STOCK_PER_WAREHOUSE = 1000; //origin : 100000

	//District constants
	public static final int DISTRICTS_PER_WAREHOUSE = 10;
	public static final double INITIAL_D_YTD = 30000.00; //different from Warehouse
	public static final int INITIAL_NEXT_O_ID = 31; // origin : 3001

	//Customer constants
	public static final int CUSTOMERS_PER_DISTRICT = 30; // origin : 3000
	public static final double INITIAL_CREDIT_LIM = 50000.00;
	public static final double MIN_DISCOUNT = 0.0000;
	public static final double MAX_DISCOUNT = 0.5000;
	public static final int DISCOUNT_DECIMALS = 4;
	public static final double INITIAL_BALANCE = -10.00;
	public static final double INITIAL_YTD_PAYMENT = 10.00;
	public static final int INITIAL_PAYMENT_CNT = 1;
	public static final int INITIAL_DELIVERY_CNT = 0;
	public static final int MIN_FIRST = 6;
	public static final int MAX_FIRST = 10;
	public static final String MIDDLE = "OE";
	public static final int PHONE = 16;
	public static final int MIN_C_DATA = 300;
	public static final int MAX_C_DATA = 500;
	public static final String GOOD_CREDIT = "GC";
	public static final String BAD_CREDIT = "BC";
	
	//Order constants
	public static final int MIN_CARRIER_ID = 1;
	public static final int MAX_CARRIER_ID = 10;
	//HACK: This is not strictly correct, but it works
	public static final int NULL_CARRIER_ID = 0;
	//o_id < than this value, carrier != null, >= -> carrier == null
	public static final int NULL_CARRIER_LOWER_BOUND = 2101;
	public static final int MIN_OL_CNT = 5;
	public static final int MAX_OL_CNT = 15;
	public static final boolean INITIAL_ALL_LOCAL = true;
	public static final int INITIAL_ORDERS_PER_DISTRICT = 30; // origin : 3000

	//Used to generate new order transactions
	public static final int MAX_OL_QUANTITY = 10;

	//Order line constants
	public static final int INITIAL_QUANTITY = 5;
	public static final double MIN_AMOUNT = 0.01;

	//History constants
	public static final int MIN_DATA = 12;
	public static final int MAX_DATA = 24;
	public static final double INITIAL_AMOUNT = 10.00;

	//New order constants
	public static final int INITIAL_NEW_ORDERS_PER_DISTRICT = 9; // origin : 900

	//TPC-C 2.4.3.4 (page 31) says this must be displayed when new order rolls back.
	public static final String INVALID_ITEM_MESSAGE = "Item number is not valid";

	//Used to generate stock level transactions
	public static final int MIN_STOCK_LEVEL_THRESHOLD = 10;
	public static final int MAX_STOCK_LEVEL_THRESHOLD = 20;

	//Used to generate payment transactions
	public static final double MIN_PAYMENT = 1.0;
	public static final double MAX_PAYMENT = 5000.0;

	//Indicates "brand" items and stock in i_data and s_data.
	public static final String ORIGINAL_STRING = "ORIGINAL";

	//Table Names
	public static final String TABLENAME_ITEM       = "ITEM";
	public static final String TABLENAME_WAREHOUSE  = "WAREHOUSE";
	public static final String TABLENAME_DISTRICT   = "DISTRICT";
	public static final String TABLENAME_CUSTOMER   = "CUSTOMER";
	public static final String TABLENAME_STOCK      = "STOCK";
	public static final String TABLENAME_ORDERS     = "ORDERS";
	public static final String TABLENAME_NEW_ORDER  = "NEW_ORDER";
	public static final String TABLENAME_ORDER_LINE = "ORDER_LINE";
	public static final String TABLENAME_HISTORY    = "HISTORY";
}
