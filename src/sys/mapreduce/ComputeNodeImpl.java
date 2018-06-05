package sys.mapreduce;

import utils.JKS;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;

import api.mapreduce.ComputeNode;
import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import jersey.repackaged.com.google.common.collect.Lists;
import sys.storage.RemoteBlobStorage;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;


import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;




@SuppressWarnings("restriction")
@WebService(serviceName = ComputeNode.NAME, targetNamespace = ComputeNode.NAMESPACE, endpointInterface = ComputeNode.INTERFACE)
public class ComputeNodeImpl implements ComputeNode {

	private static String worker;
	private static BlobStorage storage;


	private static final String SERVER_KEYSTORE = "/home/sd/datanodeks.jks";
	private static final String SERVER_KEYSTORE_PWD = "password";

	private static final String SERVER_TRUSTSTORE = "/home/sd/datanodets.jks";
	private static final String SERVER_TRUSTSTORE_PWD = "password";

	public ComputeNodeImpl(String worker, BlobStorage storage) {
		try {
			this.worker = worker;
			this.storage = storage;
			Thread.sleep(5000);
		}catch(Exception e ){
			e.printStackTrace();
		}
	}

	@Override
	public void mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize)
			throws InvalidArgumentException {
		try {


			//Test received parameters
			if (jobClassBlob == null || inputPrefix == null || outputPrefix == null)
				throw new InvalidArgumentException();

			new MapperTask(worker, storage, jobClassBlob, inputPrefix, outputPrefix).execute();

			Set<String> reduceKeyPrefixes = storage.listBlobs(outputPrefix + "-map-").stream()
					.map(blob -> blob.substring(0, blob.lastIndexOf('-'))).collect(Collectors.toSet());

			AtomicInteger partitionCounter = new AtomicInteger(0);
			Lists.partition(new ArrayList<>(reduceKeyPrefixes), outPartSize).forEach(partitionKeyList -> {

				String partitionOutputBlob = String.format("%s-part%04d", outputPrefix, partitionCounter.incrementAndGet());

				BlobWriter writer = storage.blobWriter(partitionOutputBlob);

				partitionKeyList.forEach(keyPrefix -> {
					new ReducerTask("client", storage, jobClassBlob, keyPrefix, outputPrefix).execute(writer);
				});

				writer.close();
			});
		}catch(Exception e ){
			e.printStackTrace();
			System.err.println("CATCHH it all!!!!");
		}
	}


	public static void main(String[] args) throws Exception {

		List<X509Certificate> trustedCertificates = new ArrayList<>();

		KeyStore ks = JKS.load(SERVER_KEYSTORE, SERVER_KEYSTORE_PWD);
		KeyStore ts = JKS.load(SERVER_TRUSTSTORE, SERVER_TRUSTSTORE_PWD);

		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(ks, SERVER_KEYSTORE_PWD.toCharArray());

		SSLContext ctx = SSLContext.getInstance("TLS");

		TrustManagerFactory tmf2 = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf2.init(ts);

		for (TrustManager tm : tmf2.getTrustManagers()) {
			if (tm instanceof X509TrustManager)
				trustedCertificates.addAll(Arrays.asList(((X509TrustManager) tm).getAcceptedIssuers()));
		}

		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {

			@Override
			public void checkClientTrusted(X509Certificate[] certs, String authType) {
				System.err.println(certs[0].getSubjectX500Principal());
			}

			@Override
			public void checkServerTrusted(X509Certificate[] certs, String authType) {
				Thread.dumpStack();
			}

			@Override
			public X509Certificate[] getAcceptedIssuers() {
				return trustedCertificates.toArray(new X509Certificate[0]);
			}
		} };


		ctx.init(kmf.getKeyManagers(), trustAllCerts, null);

		HttpsServer server = HttpsServer.create(new InetSocketAddress("0.0.0.0" ,9992), -1);

		server.setHttpsConfigurator(new HttpsConfigurator(ctx));
		server.setHttpsConfigurator(new HttpsConfigurator(ctx) {

			@Override
			public void configure(HttpsParameters params) {
				SSLParameters sslparams = ctx.getDefaultSSLParameters();
				sslparams.setNeedClientAuth(true);
				params.setSSLParameters(sslparams);
			}
		});

		server.start();
		System.err.println("Server ready....");

		Endpoint endpoint = Endpoint.create(new ComputeNodeImpl("worker", new RemoteBlobStorage()));
 		endpoint.publish(server.createContext("/mapreduce/"));
		System.err.println("SOAP ComputeNode Server ready...");
	}

}
