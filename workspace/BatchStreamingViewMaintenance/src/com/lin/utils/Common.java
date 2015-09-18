package com.lin.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Common {
	// not working since all objects need to be serialized
//	public static Object deepCopy(Object obj){
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        ObjectOutputStream oos;
//		try {
//			oos = new ObjectOutputStream(bos);
//			oos.writeObject(obj);
//			ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
//			ObjectInputStream ois = new ObjectInputStream(bis);
//			return ois.readObject();
//		} catch (IOException | ClassNotFoundException e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
	
	public static String senitiseSQL(String input){
		String result = null;
		result = input.replace(' ', '_')
				.replace('.', '_')
				.replace('>', 'g')
				.replace('<', 'l')
				.replace('=', 'e')
				.replace('(', '_')
				.replace(')','_')
				.replace(',', '_');
		return result;
	}
}
