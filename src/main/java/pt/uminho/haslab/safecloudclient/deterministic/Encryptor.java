package pt.uminho.haslab.safecloudclient.deterministic;

import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;

public class Encryptor {

	public static byte[] encrypt(String key, String initVector, byte[] value) {
		try {
			IvParameterSpec iv = new IvParameterSpec(
					initVector.getBytes("UTF-8"));
			SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes("UTF-8"),
					"AES");

			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
			cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);

			byte[] encrypted = cipher.doFinal(value);
			System.out.println("encrypted string: "
					+ Base64.encodeBase64String(encrypted));

			// return Base64.encodeBase64String(encrypted);
			return encrypted;
		} catch (UnsupportedEncodingException ex) {
			System.out.println(ex);
		} catch (InvalidAlgorithmParameterException ex) {
			System.out.println(ex);
		} catch (InvalidKeyException ex) {
			System.out.println(ex);
		} catch (NoSuchAlgorithmException ex) {
			System.out.println(ex);
		} catch (BadPaddingException ex) {
			System.out.println(ex);
		} catch (IllegalBlockSizeException ex) {
			System.out.println(ex);
		} catch (NoSuchPaddingException ex) {
			System.out.println(ex);
		}

		return null;
	}

	public static byte[] decrypt(String key, String initVector, byte[] encrypted) {
		try {
			IvParameterSpec iv = new IvParameterSpec(
					initVector.getBytes("UTF-8"));
			SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes("UTF-8"),
					"AES");

			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
			cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);

			byte[] original = cipher.doFinal(Base64.decodeBase64(encrypted));

			// return new String(original);
			return original;
		} catch (UnsupportedEncodingException ex) {
			System.out.println(ex);
		} catch (InvalidAlgorithmParameterException ex) {
			System.out.println(ex);
		} catch (InvalidKeyException ex) {
			System.out.println(ex);
		} catch (NoSuchAlgorithmException ex) {
			System.out.println(ex);
		} catch (BadPaddingException ex) {
			System.out.println(ex);
		} catch (IllegalBlockSizeException ex) {
			System.out.println(ex);
		} catch (NoSuchPaddingException ex) {
			System.out.println(ex);
		}

		return null;
	}
}
