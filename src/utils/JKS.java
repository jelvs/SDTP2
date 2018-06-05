package utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

public class JKS {

	public static KeyStore load( String filename, String password ) throws Exception {
		try (InputStream ksIs = new FileInputStream(filename)) {
			KeyStore ks = KeyStore.getInstance("JKS");
			ks.load(ksIs, password.toCharArray());
			return ks;
		}
	}
}
