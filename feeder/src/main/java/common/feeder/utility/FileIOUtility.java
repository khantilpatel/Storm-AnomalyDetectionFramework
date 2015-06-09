package common.feeder.utility;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import data.collection.entity.TweetTableObject;

public class FileIOUtility {

	public static List<List<TweetTableObject>> readTweetListFromFile(String fileName) {
		List<List<TweetTableObject>> arraylist = null;
		try {
			FileInputStream fis = new FileInputStream(fileName);
			ObjectInputStream ois = new ObjectInputStream(fis);
			arraylist = (ArrayList<List<TweetTableObject>>) ois.readObject();
			ois.close();
			fis.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (ClassNotFoundException c) {
			System.out.println("Class not found");
			c.printStackTrace();
		}

		return arraylist;
	}

	public static void saveTweetListToFile(List<List<TweetTableObject>> list, String filename) {
		try {
			FileOutputStream fos = new FileOutputStream(filename);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(list);
			oos.close();
			fos.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
