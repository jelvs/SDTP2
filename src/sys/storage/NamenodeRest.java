package sys.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import com.mongodb.client.model.Filters;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import api.storage.Datanode;
import api.storage.Namenode;
import kafka.Publisher;
import kafka.Subscriber;
import org.omg.CosNaming.NamingContextPackage.NotFound;
import utils.IP;
import utils.ServiceDiscovery;

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

    private MongoClient mongo;

    private MongoDatabase db;

    //Trie<String, List<String>> names = new PatriciaTrie<>();

    Map<String, Datanode> datanodes;



    public NamenodeRest() {
        MongoClientURI uri = new MongoClientURI("mongodb://mongo1,mongo2,mongo3/?w=majority&readConcernLevel=majority&readPreference=primary");
        try {

            mongo = new MongoClient(uri);

            db = mongo.getDatabase("testDB");

            table = db.getCollection("col");

            //create index

        } catch (Exception e) {
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
        //try {
            for (Document doc : table.find(Filters.regex("_id", "^(" + prefix + ")"))) {

                        prefixes.add((String)doc.get("_id"));


            }
            System.err.println("HELLO");
            System.err.println("PREFIXOS de " + prefix + ": " + prefixes);
            return prefixes;

        /*}catch(Exception e){
            System.err.println("CATCH");
            e.printStackTrace();
            return prefixes;
        }*/
    }

    @Override
    public synchronized void create(String name, List<String> blocks) {

            try{
                //System.err.println("CREEEEEEATTE");
                Document searchQuery = new Document();
                searchQuery.put("_id", name);
                //searchQuery.put("name", name);
                searchQuery.put("blocks", blocks);
                table.insertOne(searchQuery);
            }catch(Exception e){
                logger.info("Namenode create CONFLICT");
                throw new WebApplicationException(Status.CONFLICT);
            }

           /* if (table.find(searchQuery).first() != null) {
                logger.info("Namenode create CONFLICT");
                throw new WebApplicationException(Status.CONFLICT);
            } else {

                //System.err.println("CONFLICT ETRCYIVUBH");

            }
            System.err.println(name + "/" + blocks.size());
            throw new WebApplicationException(Status.NO_CONTENT);*/

    }

    @Override
    public synchronized void delete(String prefix) {

        Document searchQuery = new Document();
        searchQuery.put("_id", prefix);

        if (table.find(Filters.regex("_id","^(" +  prefix + ")")).first() == null) {
            logger.info("Namenode delete NOT FOUND");
            throw new WebApplicationException(Status.NOT_FOUND);
        }else{
            table.deleteMany(Filters.regex("_id", "^(" + prefix + ")"));
        }



    }

    @Override
    public synchronized void update(String name, List<String> blocks) {
        Document updateBlocks = new Document();
        updateBlocks.put("blocks", blocks);

        Document searchQuery = new Document();
        searchQuery.put("_id", name);

        if(!table.updateOne(searchQuery,updateBlocks).wasAcknowledged()){
            throw new WebApplicationException(Status.NOT_FOUND);
        }

    }

    @Override
    public synchronized List<String> read(String name) {
        ArrayList<String> results;
        Document searchQuery = new Document();
        searchQuery.put("_id", name);
        Document result = table.find(Filters.regex("_id", name)).first();

            if (result==null) {
                throw new WebApplicationException(Status.NOT_FOUND);
            }
            else{

                //System.err.println("Lista: " + (ArrayList)result.get("blocks", ArrayList.class));
                results = (ArrayList)result.get("blocks", ArrayList.class);
                System.err.println("NAME: " + name);
                for(int i=0 ; i<results.size(); i++){
                         results.get(i);
                         System.err.println("Namenode: " +  results.get(i));

                }
            }
            //System.err.println("NAMENODES : " + results.toString());
            return results;


        //}
        //throw new WebApplicationException(Status.NOT_FOUND);

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
