package org.apache.hadoop.hive.ql.log;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;

public class MemoryLogger {

  static Logger log = Logger.getLogger(MemoryLogger.class.getName());

  private static ProcessBuilder getProcessBuilder(String subCommand){
    ProcessBuilder builder = new ProcessBuilder();
    //Get own pid
    String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    builder.command("sh", "-c", String.format("jcmd %s %s", pid, subCommand));
    return builder;

  }
  private static String memoryInfo="";
  private static String steps="";
  private static String Command;

  private static String getResults(Process process){
    BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String l = null;
    String ret = "";
    try {
      while ((l = input.readLine()) != null) {
        ret += l+"\n";
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ret;
  }


  public static void printResultToCSV(){
    File file = new File(csvpath);
    try {
      FileWriter writer =new FileWriter(file, true);
      writer.append(Command+','+ steps + '\n');
      String[] lines = memoryInfo.split("\n");
      ArrayList<String> youngLines = new ArrayList<String>();
      ArrayList<String> oldLines = new ArrayList<String>();
      for(String line : lines){
        if(line.contains("PSYoungGen")){
          youngLines.add(line);
        }
        else if(line.contains("ParOldGen")){
          oldLines.add(line);
        }
      }
      ArrayList<Integer> youngValues= new ArrayList<>();
      ArrayList<Integer> oldValues= new ArrayList<>();

      writer.append("Young,");
      for(String young: youngLines){
        String result = young.substring(young.indexOf("used") + 5,young.lastIndexOf('K'));
        youngValues.add(Integer.parseInt(result));
        writer.append(result+',');
      }
      writer.append('\n');
      writer.append("Old,");
      for(String old: oldLines){
        String result = old.substring(old.indexOf("used") + 5, old.lastIndexOf('K'));
        oldValues.add(Integer.parseInt(result));
        writer.append(result+',');
      }
      writer.append('\n');
      writer.append("Sum,");
      for (int i = 0; i <youngValues.size(); i++) {
        writer.append(Integer.toString(youngValues.get(i)+oldValues.get(i))+',');
      }
      writer.append('\n');
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    memoryInfo="";
    steps="";
  }

  private static void clean(){
    System.gc();
  }

  private static String csvpath;

  public static void LoggerConfig(String fullLogPath, String csvPath, String command){
    SimpleLayout layout = new SimpleLayout();
    try {
      FileAppender appender = new FileAppender(layout, fullLogPath, false);
      log.addAppender(appender);
      csvpath=csvPath;
      Command = command;
    }catch (IOException e){
      e.printStackTrace();
    }
  }



  /**
   * Prints the current memory usage info
   */
  public static void checkMemoryInfo(String customMessage){
    clean();
    Process process = null;
    try {
      process =  getProcessBuilder("GC.heap_info").start();
    } catch (IOException e) {
      e.printStackTrace();
    }
    String temp=getResults(process);
    memoryInfo+= temp;
    steps+=customMessage+',';
    log.info(customMessage + "\n\n" + temp);
  }


  public static void createHeapDump(String path){
    try {
      clean();
      getProcessBuilder("GC.heap_dump " + path).start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Prints the top n classes that uses the most memory
   * @param n
   */
  public static void checkClassInfo(int n){
    Process process = null;
    try {
      process =  getProcessBuilder("GC.class_histogram").start();
    } catch (IOException e) {
      e.printStackTrace();
    }
    String[] lines = getResults(process).split("\n");
    String write ="";
    //+4 because the first 4 line is not class info
    for (int i = 0; i < n+4; i++) {
      write+=lines[i]+"\n";
    }
    log.info(write);
  }
}

