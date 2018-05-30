package sys.storage.dropbox;

import api.storage.Datanode;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.*;
import com.github.scribejava.core.oauth.OAuth20Service;
import kafka.Publisher;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.pac4j.scribe.builder.api.DropboxApi20;
import sys.storage.BlockIO;
import sys.storage.DatanodeRest;
import sys.storage.dropbox.msgs.*;
import utils.IP;
import utils.JSON;
import utils.ServiceDiscovery;

import javax.net.ssl.SSLContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;


public class DropboxDatanodeServer implements Datanode{
    private static final String DROPBOXDATANODE_PORT_DEFAULT = "9999";
    private static boolean kafka = false;
    private static final String apiKey = "n77x0i4pkxqs8u8";
    private static final String apiSecret = "dy0zqbsfdh5bhue";
    private static final String CREATE = "https://api.dropboxapi.com/2/files/upload";
    private static final String GET = "https://api.dropboxapi.com/2/files/download";
    private static final String LIST_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/list_folder";
    private static final String LIST_FOLDER_CONTINUE_V2_URL = "https://api.dropboxapi.com/2/files/list_folder/continue";
    private static final String CREATE_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/create_folder_v2";
    private static final String DELETE = "https://api.dropboxapi.com/2/file_requests/delete_v2";
    private static final String PATH = "/Datanode/";
    private static final String TOKEN = "85W107GG8ZgAAAAAAAAEGt4jS8VXi9Dkr6vYu3lkYyQVJG_XIvs7QaFTTugpfjHy";

    private String path;

    protected static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
    protected static final String OCTET_CONTENT_TYPE = "application/octet-stream; charset=utf-8";
    protected static final String ADD = "add";

    protected OAuth20Service service;
    protected OAuth2AccessToken accessToken;
    private String address;

    protected DropboxDatanodeServer(String myURL){
        this.address = myURL;
        try {
            OAuthCallbackServlet.start();
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

    @Path("/")
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
            }catch(Exception e){
                System.err.println("CATCH!!");
                e.printStackTrace();
            }
        }
    }

    public String createBlock(byte[] data){
        try {
            //System.err.println("LETS TRY");
            OAuthRequest createFile = new OAuthRequest(Verb.POST, CREATE);
            createFile.addHeader("Content-Type", OCTET_CONTENT_TYPE);
            createFile.addHeader("Dropbox-API-arg", JSON.encode(new CreateFileArgs(path, ADD, false, false)));
            createFile.setPayload(data);
            service.signRequest(accessToken, createFile);

            Response r = service.execute(createFile);
            System.err.println("SIGNREQUEST");
            CreateFileReturn res = JSON.decode(r.getBody(), CreateFileReturn.class);
            System.err.println("after");
            if (r.getCode() == 409) {
                System.err.println("Dropbox file already exists");
                return null;
            } else if (r.getCode() == 200) {
                System.err.println("Dropbox file was created with success");
                return res.getPath();
                //return path;
            } else {
                System.err.println("Unexpected error HTTP: " + r.getCode());
                return null;
            }
        }catch(Exception e ){
            System.err.println("CATCH IT ALL!");
            e.printStackTrace();
            return null;

        }


    }

    public byte[] readBlock(String path){
        try {
            OAuthRequest getFile = new OAuthRequest(Verb.POST, GET);
            getFile.addHeader("Content-Type", JSON_CONTENT_TYPE);
            getFile.addHeader("Dropbox-API-arg", JSON.encode(new GetFileArgs(path)));

            byte[] response;
            service.signRequest(accessToken, getFile);
            Response r = service.execute(getFile);
            if (r.getCode() == 200) {
                System.err.println("Dropbox file downloaded with success");
                response = r.getBody().getBytes();
                return response;
            } else {
                System.err.println("Unexpected error HTTP:" + r.getCode());
                return null;
            }
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }


    }

    public void deleteBlock(String path) {
        try {
            OAuthRequest deleteFile = new OAuthRequest(Verb.POST, DELETE);
            deleteFile.addHeader("Content-Type", JSON_CONTENT_TYPE);

            deleteFile.setPayload(JSON.encode(new DeleteFilesArgs(path)));

            service.signRequest(accessToken, deleteFile);
            Response r = service.execute(deleteFile);

            if (r.getCode() == 200) {
                System.err.println("Dropbox file deleted with success");
            } else {
                System.err.println("Unexpected error HTTP:" + r.getCode());
            }
        }catch(Exception e) {
            e.printStackTrace();
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
    public static void main(String[] args){
        try {
            System.setProperty("java.net.preferIPv4Stack", "true");

            String port =  DROPBOXDATANODE_PORT_DEFAULT;
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
                Publisher pub = new Publisher("Datanode", myAddress + "/");
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}