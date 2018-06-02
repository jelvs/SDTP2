package utils;

public class MapReduceCommon2 {

	public static final Object READ_TIMEOUT = 2000;
	public static final Object CONNECT_TIMEOUT = 2000;

	public static String[] getAddressFromBlockUUID(String block, String datanode) {
		String[] ret = null;
		String[] cur = block.split(" ");
		String urlToReturn = null;
		
		String blockUrl = cur[0];
		if(blockUrl.contains(datanode)) {
			blockUrl = cur[1];
		}
		
		
		int idx = blockUrl.lastIndexOf("/datanode");
	
		if (idx > 0) {
			ret = new String[]{block.substring(0, idx+1), block.substring(idx+10)};
		}
		
		return ret;
	}
	
}
