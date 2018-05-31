package sys.storage.dropbox;

import api.storage.Datanode;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import kafka.Publisher;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.pac4j.scribe.builder.api.DropboxApi20;
import sys.storage.dropbox.msgs.*;
import utils.IP;
import utils.JSON;
import utils.ServiceDiscovery;
import javax.ws.rs.core.Response.Status;

import javax.net.ssl.SSLContext;
import javax.ws.rs.WebApplicationException;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;


public class DropboxDatanodeServer implements Datanode {
    private static final String DROPBOXDATANODE_PORT_DEFAULT = "9999";
    private static boolean kafka = false;
    private static final String apiKey = "n77x0i4pkxqs8u8";
    private static final String apiSecret = "dy0zqbsfdh5bhue";
    private static final String CREATE = "https://content.dropboxapi.com/2/files/upload";
    private static final String GET = "https://content.dropboxapi.com/2/files/download";
    private static final String LIST_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/list_folder";
    private static final String LIST_FOLDER_CONTINUE_V2_URL = "https://api.dropboxapi.com/2/files/list_folder/continue";
    private static final String CREATE_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/create_folder_v2";
    private static final String DELETE = "https://api.dropboxapi.com/2/files/delete_v2";
    private static final String PATH = "/datanode/";
    private static final String TOKEN = "85W107GG8ZgAAAAAAAAEHhVRsBQO8_GepwhW13OvYnbRuFz1WMB6j1OJPlQ94FlD";

    private String path;

    protected static final String JSON_CONTENT_TYPE = "application/json";
    protected static final String OCTET_CONTENT_TYPE = "application/octet-stream";

    protected OAuth20Service service;
    protected OAuth2AccessToken accessToken;
    private String address;
    private static Logger logger = Logger.getLogger(Datanode.class.getName());

    protected DropboxDatanodeServer(String myURL) {
        this.address = myURL;
        //path = PATH + "/";
        try {
            //OAuthCallbackServlet.start();
            service = new ServiceBuilder().apiKey(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
            //.callback(OAuthCallbackServlet.CALLBACK_URI)
            accessToken = new OAuth2AccessToken(TOKEN);



            /*try {
                ObjectInputStream in = new ObjectInputStream(new FileInputStream("key.txt"));

            } catch (FileNotFoundException e) {

            }

            String authorizationURL = service.getAuthorizationUrl();
            System.out.println("Open the following URL in a browser:\n" + authorizationURL);

            String authorizationID;
            try (Scanner sc = new Scanner(System.in)) {
                System.out.print("\n\nAuthorization code: ");
                authorizationID = sc.nextLine();
            }
            accessToken = service.getAccessToken(authorizationID);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("key.txt"));
                out.writeObject(accessToken);
                out.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            path = PATH ;//+ utils.Random.key64();*/


        } catch (Exception x) {
            x.printStackTrace();
            System.exit(0);
        }
    }

    /*@Path("/")
    public static class OAuthCallbackServlet {
        public static final String CALLBACK_URI = "https://localhost:5555/";


        @GET
        public String callback(@QueryParam("code") String code) {
            return String.format("<html>Authorization-Code: %s</html>", code);
        }


        public static void start() {
            try {


                ResourceConfig config = new ResourceConfig();
                config.register(new OAuthCallbackServlet());
                JdkHttpServerFactory.createHttpServer(URI.create(CALLBACK_URI), config, SSLContext.getDefault());
            } catch (Exception e) {
                System.err.println("CATCH!!");
                e.printStackTrace();
            }
        }
    }
    */
    public synchronized String createBlock(byte[] data) {
        try {
            //System.out.println("Data: " + Base64.getEncoder().encodeToString(data));
            OAuthRequest createFile = new OAuthRequest(Verb.POST, CREATE);
            createFile.addHeader("Content-Type", OCTET_CONTENT_TYPE);
            createFile.addHeader("Dropbox-API-Arg", JSON.encode(new CreateFileArgs( PATH + utils.Random.key64(), "add", true, false)));
            createFile.setPayload(data);
            service.signRequest(accessToken, createFile);
            Response r = service.execute(createFile);

            CreateFileReturn result = JSON.decode(r.getBody(), CreateFileReturn.class);
            if (r.getCode() == 409) {
                return null;
            } else if (r.getCode() == 200) {
                return address + result.getPath();

            } else {
                return null;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return null;


        }


    }

    public synchronized byte[] readBlock(String path) {
        byte[] response;
        try {
            OAuthRequest getFile = new OAuthRequest(Verb.GET, GET);
            getFile.addHeader("Dropbox-API-Arg", JSON.encode(new GetFileArgs(PATH + path)));
            service.signRequest(accessToken, getFile);
            Response r = service.execute(getFile);

            if (r.getCode() == 409) {
                throw new WebApplicationException(Status.NOT_FOUND);
            } else if (r.getCode() == 200) {
                //System.err.println("Unexpected error HTTP:" + r.getCode());
                System.err.println("BODY : " + r.getBody());
                response = r.getBody().getBytes();
                return response;
            }else{
                System.err.println("ELSE " + r.getCode());
                System.err.println("MESSAGE : " + r.getMessage());
                return null;
            }
        } catch (Exception e) {
            System.err.println("CATCH");
            return null;
            //throw new WebApplicationException(Status.NOT_FOUND);

        }

    }


    public synchronized void deleteBlock(String path) {

        try {
            OAuthRequest deleteFile = new OAuthRequest(Verb.POST, DELETE);
            deleteFile.addHeader("Content-Type", JSON_CONTENT_TYPE);
            deleteFile.setPayload(JSON.encode(new DeleteFilesArgs(PATH + path)));
            service.signRequest(accessToken, deleteFile);
            Response r = service.execute(deleteFile);
            System.err.println("BODY : " + r.getBody());
            System.err.println("CODE : " + r.getCode());
            System.err.println("MESSAGE : " + r.getMessage());

            if (r.getCode() == 409) {
                throw new WebApplicationException(Status.NOT_FOUND);
            } else if (r.getCode() == 200) {
                System.err.println("Dropbox file was deleted with success");


            }
        } catch (Exception e) {
            //System.err.println("INSIDE CATCH");
            e.printStackTrace();
            throw new WebApplicationException(Status.NOT_FOUND);
        }


    }


    public void createDirectory(String path) throws Exception {
        OAuthRequest createFolder = new OAuthRequest(Verb.POST, CREATE_FOLDER_V2_URL);
        createFolder.addHeader("Content-Type", JSON_CONTENT_TYPE);
        createFolder.setPayload(JSON.encode(new CreateFolderV2Args(path, false)));

        service.signRequest(accessToken, createFolder);
        Response r = service.execute(createFolder);
        if (r.getCode() == 409) {
            System.err.println("Dropbox directory already exists");
        } else if (r.getCode() == 200) {
            System.err.println("Dropbox directory was created with success");
            return;
        } else {
            System.err.println("Unexpected error HTTP: " + r.getCode());
        }
    }


    public void listDirectory(String path) throws Exception {
        OAuthRequest listFolder = new OAuthRequest(Verb.POST, LIST_FOLDER_V2_URL);
        listFolder.addHeader("Content-Type", JSON_CONTENT_TYPE);
        listFolder.setPayload(JSON.encode(new ListFolderV2Args(path, true)));

        for (; ; ) {
            service.signRequest(accessToken, listFolder);
            Response r = service.execute(listFolder);
            if (r.getCode() != 200)
                throw new RuntimeException("Failed: " + r.getMessage());

            ListFolderV2Return result = JSON.decode(r.getBody(), ListFolderV2Return.class);
            result.getEntries().forEach(System.out::println);

            if (result.has_more()) {
                System.err.println("continuing...");
                listFolder = new OAuthRequest(Verb.POST, LIST_FOLDER_CONTINUE_V2_URL);
                listFolder.addHeader("Content-Type", JSON_CONTENT_TYPE);
                listFolder.setPayload(JSON.encode(new ListFolderContinueV2Args(result.getCursor())));
            } else
                break;
        }
    }

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) {
        try {
            System.setProperty("java.net.preferIPv4Stack", "true");

            String port = DROPBOXDATANODE_PORT_DEFAULT;
            if (args.length > 0 && args[0] != null) {
                port = args[0];
            }
            String URI_BASE = "https://0.0.0.0:" + port + "/";
            ResourceConfig config = new ResourceConfig();
            String myAddress = "https://" + IP.hostAddress() + ":" + port;
            config.register(new DropboxDatanodeServer(myAddress));
            JdkHttpServerFactory.createHttpServer(URI.create(URI_BASE), config, SSLContext.getDefault());

            System.err.println("DropboxDatanode ready....");
            if (!kafka) {

                ServiceDiscovery.multicastReceive(ServiceDiscovery.DATANODE_SERVICE_NAME, myAddress + "/");
                //System.err.println("OLA");
            } else {
                Publisher pub = new Publisher("datanode", myAddress + "/");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}