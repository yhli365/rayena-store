package com.run.ayena.store.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;

/**
 * @author Yanhong Lee
 * 
 */
public class ValueUtils {
	public static final Charset UTF8 = Charset.forName("UTF-8");

	private static final byte[] HEX_CHAR_TABLE = { (byte) '0', (byte) '1',
			(byte) '2', (byte) '3', (byte) '4', (byte) '5', (byte) '6',
			(byte) '7', (byte) '8', (byte) '9', (byte) 'a', (byte) 'b',
			(byte) 'c', (byte) 'd', (byte) 'e', (byte) 'f' };

	private static final byte[] chars;
	private static final byte[] charsUpper;
	private static final Random rchars = new Random();

	static {
		List<Byte> list = new ArrayList<Byte>();
		List<Byte> listUpper = new ArrayList<Byte>();

		for (int i = 'a'; i <= 'z'; i++) {
			list.add(Byte.valueOf((byte) i));
		}
		for (int i = 'A'; i <= 'Z'; i++) {
			list.add(Byte.valueOf((byte) i));
			listUpper.add(Byte.valueOf((byte) i));
		}
		for (int i = '0'; i <= '9'; i++) {
			list.add(Byte.valueOf((byte) i));
		}

		Collections.shuffle(list);
		chars = new byte[list.size()];
		for (int i = 0; i < list.size(); i++) {
			chars[i] = list.get(i);
		}

		Collections.shuffle(listUpper);
		charsUpper = new byte[listUpper.size()];
		for (int i = 0; i < listUpper.size(); i++) {
			charsUpper[i] = listUpper.get(i);
		}
	}

	public static String toHexString(byte[] raw)
			throws UnsupportedEncodingException {
		byte[] hex = new byte[2 * raw.length];
		int index = 0;

		for (byte b : raw) {
			int v = b & 0xFF;
			hex[index++] = HEX_CHAR_TABLE[v >>> 4];
			hex[index++] = HEX_CHAR_TABLE[v & 0xF];
		}
		return new String(hex, "ASCII");
	}

	public static byte[] parseIntBytes(String value) {
		if (StringUtils.isEmpty(value)) {
			return null;
		}
		String[] arr = value.split(",");
		byte[] bytes = new byte[arr.length];
		for (int i = 0; i < arr.length; i++) {
			bytes[i] = (byte) Integer.parseInt(arr[i]);
		}
		return bytes;
	}

	public static String formatIntBytes(byte[] bytes) {
		StringBuilder sb = new StringBuilder();
		for (byte b : bytes) {
			sb.append(",");
			if (b < 0) {
				sb.append(256 + b);
			} else {
				sb.append(b);
			}
		}
		return sb.substring(1);
	}

	public static long toValueIntBytes(byte[] bytes) {
		long num = 0;
		for (int i = 0; i < bytes.length; i++) {
			int v = bytes[i];
			if (v < 0) {
				v += 256;
			}
			num = (num * 255) + v;
		}
		return num;
	}

	public static byte[] randStringValue(int len) {
		byte[] result = new byte[len];
		while (len > 0) {
			len--;
			result[len] = chars[rchars.nextInt(chars.length)];
		}
		return result;
	}

	public static byte[] randUppercaseValue(int len) {
		byte[] result = new byte[len];
		while (len > 0) {
			len--;
			result[len] = charsUpper[rchars.nextInt(charsUpper.length)];
		}
		return result;
	}

	public static byte[] md5(byte[] buf) throws IOException {
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			return md5.digest(buf);
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("md5 error", e);
		}
	}

	public static byte[] md5(String msg) throws IOException {
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			return md5.digest(msg.getBytes(UTF8));
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("md5 error", e);
		}
	}

	public static String md5str(byte[] bytes) throws IOException {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] md5 = md.digest(bytes);
			return toHexString(md5);
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("md5 error", e);
		}
	}

}
