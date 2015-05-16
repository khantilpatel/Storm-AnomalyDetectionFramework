package algo.ad.utility;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FilesUtil {
  public static String readTextFile(String fileName) throws IOException {
    String content = new String(Files.readAllBytes(Paths.get(fileName)));
    return content;
  }
  
//  public static List<String> readTextFileByLines(String fileName) throws IOException {
//    //List<String> lines = Files.readAllLines(Paths.get(fileName));
//    return lines;
//  }
//  
  public static void writeToNewTextFile(String fileName, String content) throws IOException {
    Files.write(Paths.get(fileName), content.getBytes(), StandardOpenOption.CREATE);
  }
  
  public static void appendToTextFile(String fileName, String content) throws IOException {
	    Files.write(Paths.get(fileName), content.getBytes(), StandardOpenOption.APPEND);
	  }
  
} 