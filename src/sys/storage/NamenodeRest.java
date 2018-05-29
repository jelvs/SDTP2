package sys.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import api.storage.Datanode;
import api.storage.Namenode;
import kafka.Publisher;
import kafka.Subscriber;
import utils.IP;
import utils.ServiceDiscovery;

public class NamenodeRest implements Namenode {

	private static final String NAMENODE_PORT_DEFAULT = "9981";
	
	private static Logger logger = Logger.getLogger(NamenodeClient.class.toString());
	
	private static boolean kafka = false;

	Trie<String, List<String>> names = new PatriciaTrie<>();

	Map<String, Datanode> datanodes;
	
	Set<String> registeredBlocks;
	
	public NamenodeRest() {
		registeredBlocks = new ConcurrentSkipListSet<String>();
		datanodes = new ConcurrentHashMap<String,Datanode>();
		
		if(!kafka) {
		Thread dataNodeDiscovery = new Thread() {
			public void run() {
				while(true) {
					String[] datanodeNames = ServiceDiscovery.multicastSend(ServiceDiscovery.DATANODE_SERVICE_NAME);
					if(datanodeNames != null) {
						for(String datanode: datanodeNames) {
							if(!datanodes.containsKey(datanode)) {
								logger.info("New Datanode discovered: " + datanode);
								datanodes.put(datanode, new DatanodeClient(datanode));
							}
						}
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						//nothing to be done
					}
				}
			}
		};
		dataNodeDiscovery.start();
		}
else {
			
			Thread dataNodeKafkaDiscovery = new Thread() {
				
				public void run() {
					Collection<String> topics = new ArrayList<String>(Arrays.asList(new String[] { "Datanode" }));
					Subscriber sub = new Subscriber(topics);
					while(true) {
						ConsumerRecords<String,String> records = sub.info();
						if(records != null) {
							records.forEach(datanode -> {
								if(!datanodes.containsKey(datanode.value())) {
									logger.info("New Datanode discovered: " + datanode.value());
									datanodes.put(datanode.value(), new DatanodeClient(datanode.value()));
								}
							}); 	
						}
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							//nothing to be done
						}
						
					}
					 
				}
				
			};
			dataNodeKafkaDiscovery.start();
			
		}
		
	}
	
	
	@Override
	public synchronized List<String> list(String prefix) {
		return new ArrayList<>(names.prefixMap(prefix).keySet());
	}

	@Override
	public synchronized void create(String name, List<String> blocks) {
		if (names.putIfAbsent(name, new ArrayList<>(blocks)) != null) {
			logger.info("Namenode create CONFLICT");
			throw new WebApplicationException( Status.CONFLICT );
		} else {
			//Remember the blocks that were added as part of the blob
			for(String block: blocks) {
				registeredBlocks.add(block);
			}
		}
	}

	@Override
	public synchronized void delete(String prefix) {		
		Set<String> keys = new HashSet<String>(names.prefixMap(prefix).keySet());
		if (!keys.isEmpty())
			for(String key: keys) {
				List<String> removedBlocks = names.remove(key);
				//Forget the blocks of the blob that was removed.
				for(String block: removedBlocks) {
					registeredBlocks.remove(block);
				}
			}
		else {
			logger.info("Namenode delete NOT FOUND");
			throw new WebApplicationException( Status.NOT_FOUND );
		}
	}

	@Override
	public synchronized void update(String name, List<String> blocks) {
		if(names.containsKey(name)) {
			List<String> oldBlocks = names.put(name, blocks);
			//Blocks that were removed as part of this update are forgotten
			for(String block: oldBlocks) {
				if(!blocks.contains(block)) {
					registeredBlocks.remove(block);
				}
			}
			//New blocks that were added as part of this update are registered
			for(String block: blocks) {
				if(!oldBlocks.contains(block)) {
					registeredBlocks.add(block);
				}
			}
		} else {
			logger.info("Namenode update NOT FOUND");
			throw new WebApplicationException( Status.NOT_FOUND );
		}
	}

	@Override
	public synchronized List<String> read(String name) {
		List<String> blocks = names.get(name);
		if (blocks == null)
			logger.info("Namenode read NOT FOUND");
		else
			logger.info("Blocks for Blob: " + name + " : " + blocks);
		return blocks;
	}

	public static void main(String[] args) throws UnknownHostException, URISyntaxException {
		System.setProperty("java.net.preferIPv4Stack", "true");
		String port = NAMENODE_PORT_DEFAULT;
		if (args.length > 0 && args[0] != null) {
			port = args[0];
		}
		String URI_BASE = "https://0.0.0.0:" + port + "/";
		String myAddress = "https://" + IP.hostAddress() + ":" + port;
		ResourceConfig config = new ResourceConfig();
		config.register(new NamenodeRest());
		JdkHttpServerFactory.createHttpServer(URI.create(URI_BASE), config);

		System.err.println("Namenode ready....");
		if(!kafka) {
		ServiceDiscovery.multicastReceive(ServiceDiscovery.NAMENODE_SERVICE_NAME, myAddress+"/");
		}
		else {
			Publisher pub = new Publisher("Namenode", myAddress + "/");	
		}
	}

}
