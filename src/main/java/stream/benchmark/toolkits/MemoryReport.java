package stream.benchmark.toolkits;

public class MemoryReport {
	public static void reportStatus(){
		final long usedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		System.out.println("******memory status******");
		System.out.println("used memory="+usedMem/1024/1024 +"MB");
		System.out.println("Max memory="+Runtime.getRuntime().maxMemory()/1024/1024+"MB");
		System.out.println("*************************");
	}
}
