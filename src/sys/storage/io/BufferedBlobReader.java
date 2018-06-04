package sys.storage.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import api.storage.BlobStorage.BlobReader;
import api.storage.Datanode;
import api.storage.Namenode;
import utils.MapReduceCommon;
import utils.MapReduceCommon2;

/*
 * 
 * Implements BlobReader.
 * 
 * Allows reading or iterating the lines of Blob one at a time, by fetching each block on demand.
 * 
 * Intended to allow streaming the lines of a large blob (with mamy (large) blocks) without reading it all first into memory.
 */
public class BufferedBlobReader implements BlobReader {

	final String name;
	final Namenode namenode; 
	final Map<String, Datanode> datanodes;
	
	final Iterator<String> blocks;

	final LazyBlockReader lazyBlockIterator;
	private boolean what = false;
	private String deadDatanode = null;
	
	private static Logger logger = Logger.getLogger(BufferedBlobReader.class.getName());
	
	public BufferedBlobReader( String name, Namenode namenode, Map<String, Datanode> datanodes ) {
		this.name = name;
		this.namenode = namenode;
		this.datanodes = datanodes;
		
		this.blocks = this.namenode.read( name ).iterator();
		this.lazyBlockIterator = new LazyBlockReader();
	}
	
	@Override
	public String readLine() {		
		String ret = lazyBlockIterator.hasNext() ? lazyBlockIterator.next() : null ;
		logger.info("readLine(): Ret -> " + ret );
		return ret;
	}
	
	@Override
	public Iterator<String> iterator() {
		return lazyBlockIterator;
	}
	
	private Iterator<String> nextBlockLines() {
		if( blocks.hasNext() )
			return fetchBlockLines( blocks.next() ).iterator();
		else 
			return Collections.emptyIterator();
	} 

	private List<String> fetchBlockLines(String block) {
		what = false;
		logger.info("accessing block on URL: " + block);
		try { 
		String[] components = MapReduceCommon.getAddressFromBlockUUID(block);
		logger.info("datanode:" + components[0] + " " + components[1]+ "RETURNED THIS SHIT");
		logger.info(deadDatanode);
		if(deadDatanode != null && deadDatanode.contains(components[0])) {
			what = true;
		}
		Datanode datanode = datanodes.get(components[0]);
		String dub = components[0];
		if(datanode == null) {
			logger.info("Unknown datanode: " + components[0]);
			what = true;
		logger.info("Known datanodes:");
		for(String dn: datanodes.keySet()) {
			logger.info(dn);
		}
		}
		logger.info("Trying to access target block: " + components[1]);
		byte[] data;
		if((what == false) && (data = datanode.readBlock( components[1] ))!= null)  {
			return Arrays.asList( new String(data).split("\\R"));
		}
		else {
			if(deadDatanode == null) {
				deadDatanode = components[0];
			}
			what = true;
			logger.info("datanode is dead");
			String[] components1 = MapReduceCommon2.getAddressFromBlockUUID(block, dub);
			logger.info("datanode:" + components1[0] + " " + components1[1]);
			logger.info("Trying to access target block: HEL00000000" + components1[1]);
			datanode = datanodes.get(components1[0]);
			byte[] data1 = datanode.readBlock( components1[1] );
			return Arrays.asList( new String(data1).split("\\R"));
			
		}
		
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<String>();
		}
	}
	
	private class LazyBlockReader implements Iterator<String> {
		
		Iterator<String> lines;
		
		LazyBlockReader() {
			this.lines = nextBlockLines();
		}
		
		@Override
		public String next() {
			return lines.next();
		}

		@Override
		public boolean hasNext() {
			return lines.hasNext() || (lines = nextBlockLines()).hasNext();
		}	
	}
}

