package stream.benchmark.tpcc.spout;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class BenchmarkRandom {

	static final String[] SYLLABLES = new String[] { "BAR", "OUGHT", "ABLE",
			"PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };

	static private Random _rand = new Random();
	static private int cLast = getNumber(0, 255);
	static private int cId = getNumber(0, 1023);
	static private int orderLineItemId = getNumber(0, 8191);

	static public int NURand(int a, int x, int y) {
		// A non-uniform random number, as defined by TPC-C 2.1.6. (page 20).
		int c = 0;
		if (a == 255) {
			c = cLast;
		} else if (a == 1023) {
			c = cId;
		} else if (a == 8191) {
			c = orderLineItemId;
		}
		return (((getNumber(0, a) | getNumber(x, y)) + c) % (y - x + 1)) + x;
	}

	static public Set<Integer> selectUniqueIds(int numUnique, int minimum,
			int maximum) {
		Set<Integer> rows = new HashSet<Integer>();
		for (int i = 0; i < numUnique; ++i) {
			Integer num = null;
			while (num == null || rows.contains(num)) {
				num = getNumber(minimum, maximum);
			}
			rows.add(num);
		}
		return rows;
	}

	static public int getNumber(int minimum, int maximum) {
		return _rand.nextInt(maximum - minimum + 1) + minimum;
	}

	static public double getFixedPoint(int decimal_places, double minimum,
			double maximum) {
		int multiplier = 1;
		for (int i = 0; i < decimal_places; ++i) {
			multiplier *= 10;
		}

		int int_min = (int) (minimum * multiplier + 0.5);
		int int_max = (int) (maximum * multiplier + 0.5);

		return (double) (getNumber(int_min, int_max) / (double) multiplier);
	}

	static public int getNumberExcluding(int minimum, int maximum, int excluding) {
		int num = getNumber(minimum, maximum - 1);
		if (num >= excluding) {
			num += 1;
		}
		return num;
	}

	static public String getAstring(int minimum_length, int maximum_length) {
		// A random alphabetic string with length in range [minimum_length,
		// maximum_length].
		return getRandomString(minimum_length, maximum_length, 'a', 26);
	}

	static public String getNstring(int minimum_length, int maximum_length) {
		return getRandomString(minimum_length, maximum_length, '0', 10);
	}

	static public String getRandomString(int minimum_length,
			int maximum_length, char base, int numCharacters) {
		int length = getNumber(minimum_length, maximum_length);
		int baseByte = (int) base;
		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; ++i) {
			sb.append((char) (baseByte + getNumber(0, numCharacters - 1)));
		}
		return sb.toString();
	}

	static public String makeLastName(int number) {
		// A last name as defined by TPC-C 4.3.2.3. Not actually random.
		return SYLLABLES[number / 100] + SYLLABLES[(number / 10) % 10]
				+ SYLLABLES[number % 10];
	}

	static public String makeRandomLastName(int maxCID) {
		// A non-uniform random last name, as defined by TPC-C 4.3.2.3.
		// The name will be limited to maxCID.
		int min_cid = 999;
		if (maxCID - 1 < min_cid) {
			min_cid = maxCID - 1;
		}
		return makeLastName(NURand(255, 0, min_cid));

	}
}