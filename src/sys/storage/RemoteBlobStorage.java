package sys.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import api.storage.BlobStorage;
import api.storage.Datanode;
import api.storage.Namenode;
import kafka.Subscriber;
import sys.storage.io.BufferedBlobReader;
import sys.storage.io.BufferedBlobWriter;
import utils.MapReduceCommon;
import utils.ServiceDiscovery;

public class RemoteBlobStorage implements BlobStorage {
	private static final int BLOCK_SIZE = 512;

	private static Logger logger = Logger.getLogger(RemoteBlobStorage.class.getName());

	private static boolean kafka = false;

	Namenode namenode;
	Map<String, Datanode> datanodes;

	public RemoteBlobStorage() {
		datanodes = new ConcurrentHashMap<String,Datanode>();
		if(!kafka) {

			Thread dataNodeDiscovery = new Thread() {
				ArrayList<String> datanodesAlive = new ArrayList<>();
				double currentTime = System.currentTimeMillis();
				public void run() {
					while(true) {
						String[] datanodeNames = ServiceDiscovery.multicastSend(ServiceDiscovery.DATANODE_SERVICE_NAME);
						if(datanodeNames != null) {

							for(String datanode: datanodeNames) {
								if (!datanodesAlive.contains(datanode)) {
									datanodesAlive.add(datanode);
								}

								if(!datanodes.containsKey(datanode)) {
									logger.info("New Datanode discovered: " + datanode);
									datanodes.put(datanode, new DatanodeClient(datanode));
								}
							}
						}

						//5 second is enough time to ping man
						if(System.currentTimeMillis() - currentTime > 5000) {
							ArrayList<String> deadDatanodes = new ArrayList<>();
							logger.info("BLOBSTORAGE datanodes check");

							datanodes.keySet().forEach(datanode -> {
								if(!datanodesAlive.contains(datanode)) {
									deadDatanodes.add(datanode);
									datanodes.remove(datanode);
									logger.info("blobstorage datanodeMissing: " + datanode);
								}
							});
							
							//deadDatanodes is set
							deadDatanodes.forEach(datanode -> {
								namenode.list("").forEach(blob -> {
									List<String> blocks = new LinkedList<>();
									namenode.read(blob).forEach(block -> {
										int idx = block.lastIndexOf(" ");
										String loc0 = block.substring(0, idx);
										String loc1 = block.substring(idx);


										if(loc0.contains(datanode)) {
											if(!loc1.contains(datanode) && loc1 != " ") {
												int urlIdx = loc1.lastIndexOf("/datanode");
												String url = loc1.substring(1, urlIdx+1);
												logger.info("RETURNING THIS BUG " + url);
												String blockName = loc1.substring(urlIdx+10);
												byte[] data = datanodes.get(url).readBlock(blockName);
												String newDatanode = randomDatanode();
												if(datanodes.size() >= 2) {
													for(;;) {
														if(newDatanode != url)
															break;
														newDatanode = randomDatanode();
													}
												}

												String blockRef = datanodes.get(newDatanode).createBlock(data);
												blocks.add(loc1 + " " + blockRef);	
											}
										}
										else {
											if(loc1.contains(datanode)) {
												if(!loc0.contains(datanode) && loc0 != " ") {
													int urlIdx = loc0.lastIndexOf("/datanode");
													String url = loc0.substring(0, urlIdx+1);
													logger.info("RETURNING THIS BUG" + url);
													String blockName = loc0.substring(urlIdx+10);
													byte[] data = datanodes.get(url).readBlock(blockName);
													String newDatanode = randomDatanode();
													if(datanodes.size() >= 2) {
														for(;;) {
															if(newDatanode != url)
																break;
															newDatanode = randomDatanode();
														}
													}

													String blockRef = datanodes.get(newDatanode).createBlock(data);
													blocks.add(loc0 + " " + blockRef);	
												}
											}
											else
												blocks.add(block);
										}

									});
									namenode.update(blob, blocks);
								});
							});






							//now we only have running datanodes
							currentTime = System.currentTimeMillis();
							datanodesAlive.clear();
							deadDatanodes.clear();
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

			while(true) {
				String[] namenodename = ServiceDiscovery.multicastSend(ServiceDiscovery.NAMENODE_SERVICE_NAME);
				if(namenodename != null) {
					logger.info("Namenode discovered: " + namenodename[0]);
					this.namenode = new NamenodeClient(namenodename[0]);
				}
				break;
			}	
		}
		else {
			Thread datanodeKafkaDiscovery = new Thread() {
				public void run() {

					Collection<String> topics = new ArrayList<String>(Arrays.asList(new String[] { "Datanode", "Namenode" }));

					Subscriber sub = new Subscriber(topics);

					while(true) {
						ConsumerRecords<String,String> records = sub.info();
						if(records != null) {
							records.forEach(record -> {
								if(record.topic().equals("Datanode")) {
									if(!datanodes.containsKey(record.value())) {
										logger.info("New Datanode discovered: " + record.value());
										datanodes.put(record.value(), new DatanodeClient(record.value()));
									}
								}
								if(record.topic().equals("Namenode")) {
									if(!namenode.equals(record.value())) {
										logger.info("Namenode discovered: " + record.value());
										namenode = new NamenodeClient(record.value());
									}
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
			datanodeKafkaDiscovery.start();
		}	
	}
	/*
	private String randomNamenode() {
		Random random = new Random();
		List<String> keys = new ArrayList<String>(namenodes.keySet());
		String randomKey = keys.get( random.nextInt(keys.size()) );

		return randomKey;
	}
	 */
	private String randomDatanode() {
		Random random = new Random();
		List<String> keys = new ArrayList<String>(datanodes.keySet());
		String randomKey = keys.get( random.nextInt(keys.size()) );

		return randomKey;
	}

	@Override
	public List<String> listBlobs(String prefix) {
		return namenode.list(prefix);
	}

	@Override
	public void deleteBlobs(String prefix) {
		namenode.list(prefix).forEach(blob -> {
			namenode.read(blob).forEach(block -> {
				String[] components = MapReduceCommon.getAddressFromBlockUUID(block);
				if(components != null && datanodes.containsKey(components[0])) {
					datanodes.get(components[0]).deleteBlock(components[1]);
				}
			});
		});
		namenode.delete(prefix);
	}


	@Override
	public BlobReader readBlob(String name) {
		logger.info("readBlob(" + name + ")");
		logger.info("Namenode is: " + ((NamenodeClient) namenode).address);
		for(String addr: datanodes.keySet()) {
			logger.info("Datanode: " + addr);
		}
		return new BufferedBlobReader(name, namenode, datanodes);
	}

	@Override
	public BlobWriter blobWriter(String name) {
		return new BufferedBlobWriter(name, namenode, datanodes, BLOCK_SIZE);
	}
	
}
