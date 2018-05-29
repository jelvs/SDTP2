package sys.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
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

import javax.net.ssl.SSLContext;
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

import java.util.Date;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class NamenodeRest implements Namenode {

    private static final String NAMENODE_PORT_DEFAULT = "9981";

    private static Logger logger = Logger.getLogger(NamenodeClient.class.toString());

    private static boolean kafka = false;

    private MongoCollection<Document> table;

    Trie<String, List<String>> names = new PatriciaTrie<>();

    Map<String, Datanode> datanodes;

    MongoClient mongo;
    MongoDatabase db;


    public NamenodeRest() {
        MongoClientURI uri = new MongoClientURI("mongodb://mongo1,mongo2,mongo3/?w=2&readPreference=secondary");
        try {

            mongo = new MongoClient(uri);

            db = mongo.getDatabase("testDB");

            table = db.getCollection("col");


        }catch (Exception e){
            System.err.println("ERROR CONNECTING TO MONGO");
        }
        datanodes = new ConcurrentHashMap<String, Datanode>();

        if (!kafka) {
            Thread dataNodeDiscovery = new Thread() {
                public void run() {
                    while (true) {
                        String[] datanodeNames = ServiceDiscovery.multicastSend(ServiceDiscovery.DATANODE_SERVICE_NAME);
                        if (datanodeNames != null) {
                            for (String datanode : datanodeNames) {
                                if (!datanodes.containsKey(datanode)) {
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
        } else {

            Thread dataNodeKafkaDiscovery = new Thread() {

                public void run() {
                    Collection<String> topics = new ArrayList<String>(Arrays.asList(new String[]{"Datanode"}));
                    Subscriber sub = new Subscriber(topics);
                    while (true) {
                        ConsumerRecords<String, String> records = sub.info();
                        if (records != null) {
                            records.forEach(datanode -> {
                                if (!datanodes.containsKey(datanode.value())) {
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
        List<String> prefixes = new ArrayList<>();
        List<Document> list = table.find().into(new ArrayList<>());
        for (Document doc : list) {
            prefixes.addAll((List<String>) doc.get(prefix));

        }
        return prefixes;
    }

    @Override
    public synchronized void create(String name, List<String> blocks) {

        System.err.println("CREEEEEEATTE");

        try {
            Document searchQuery = new Document();
            searchQuery.put("name", name);
            searchQuery.put("blocks", blocks);

            System.err.println("TABLE: " + table);

            if (table.find(searchQuery).first() != null) {
                table.insertOne(searchQuery);
            } else {
                System.err.println("CONFLICT ETRCYIVUBH");
                logger.info("Namenode create CONFLICT");
                throw new WebApplicationException(Status.CONFLICT);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("EXCEPTION CAUGHT");
        }
    }

    @Override
    public synchronized void delete(String prefix) {
        ArrayList<Document> keys = new ArrayList<>();
        List<Document> list = table.find().into(new ArrayList<>());
        for (Document doc : list) {
            keys.addAll((ArrayList) doc.get(prefix));

        }
        if (!keys.isEmpty()) {
            for (Document key : keys) {
                table.deleteOne(key);
            }
        } else {
            logger.info("Namenode delete NOT FOUND");
            throw new WebApplicationException(Status.NOT_FOUND);
        }

    }

    @Override
    public synchronized void update(String name, List<String> blocks) {
        Document oldBlocks = new Document();
        Document searchQuery = new Document();
        searchQuery.put(name, blocks);
        for (Document doc : table.find(searchQuery)) {
            oldBlocks.put(name, doc.get(name));

        }
        if (oldBlocks.isEmpty()) {
            logger.info("Namenode update NOT FOUND");
            throw new WebApplicationException(Status.NOT_FOUND);
        } else {
            table.updateOne(oldBlocks, searchQuery);
        }

    }

    @Override
    public synchronized List<String> read(String name) {
        List<String> blocks = new ArrayList<>();
        List<Document> list = table.find().into(new ArrayList<>());
        for (Document names : list) {
            blocks = (List<String>) names.get(name);
        }
        //List<String> blocks = names.get(name);
        if (blocks.isEmpty())
            logger.info("Namenode read NOT FOUND");
        else
            logger.info("Blocks for Blob: " + name + " : " + blocks);
        return blocks;
    }

    public static void main(String[] args) throws UnknownHostException, URISyntaxException, NoSuchAlgorithmException {
        System.setProperty("java.net.preferIPv4Stack", "true");
        String port = NAMENODE_PORT_DEFAULT;
        if (args.length > 0 && args[0] != null) {
            port = args[0];
        }
        String URI_BASE = "https://0.0.0.0:" + port + "/";
        String myAddress = "https://" + IP.hostAddress() + ":" + port;
        ResourceConfig config = new ResourceConfig();
        config.register(new NamenodeRest());
        JdkHttpServerFactory.createHttpServer(URI.create(URI_BASE), config, SSLContext.getDefault());

        System.err.println("Namenode ready....");
        if (!kafka) {
            ServiceDiscovery.multicastReceive(ServiceDiscovery.NAMENODE_SERVICE_NAME, myAddress + "/");
        } else {
            Publisher pub = new Publisher("Namenode", myAddress + "/");
        }
    }

}
