package bisis.export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import bisis.records.Record;
import bisis.records.serializers.PrimerakSerializer;
import bisis.records.serializers.UnimarcSerializer;
import bisis.textsrv.DBStorage;
import bisis.utils.ZipUtils;

public class Export {
  public static void main(String[] args) {
    Options options = new Options();
    options.addOption("a", "address", true,
        "MySQL server address (default: localhost)");
    options.addOption("p", "port", true, "MySQL server port (default: 3306)");
    options.addOption("d", "database", true,
        "MySQL database name (default: bisis)");
    options.addOption("u", "username", true,
        "MySQL server username (default: bisis)");
    options.addOption("w", "password", true,
        "MySQL server password (default: bisis)");
    options.addOption("o", "output", true, "Output directory");
    options.addOption("i", "incremental", false, "Indicates incremental export");
    CommandLineParser parser = new GnuParser();
    String address = "localhost";
    String port = "3306";
    String database = "bisis";
    String username = "bisis";
    String password = "bisis";
    String outputDir = "";
    boolean incremental = false;
    
    Date today = new Date();
    Date startDate = null;
    SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy.");
    String zipName = "bgb-centralna.zip";
    
    try {
      CommandLine cmd = parser.parse(options, args);
      if (cmd.hasOption("a"))
        address = cmd.getOptionValue("a");
      if (cmd.hasOption("p"))
        port = cmd.getOptionValue("p");
      if (cmd.hasOption("d"))
        database = cmd.getOptionValue("d");
      if (cmd.hasOption("u"))
        username = cmd.getOptionValue("u");
      if (cmd.hasOption("w"))
        password = cmd.getOptionValue("w");
      if (cmd.hasOption("o"))
        outputDir = cmd.getOptionValue("o");
      else
        throw new Exception("Output directory not specified.");
      if (cmd.hasOption("i"))
        incremental = true;
    } catch (Exception ex) {
      System.err.println("Invalid parameter(s), reason: " + ex.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("bisis4-export", options);
      return;
    }
    try {
      File dir = new File(outputDir);
      if (!dir.exists()) {
        dir.mkdirs();
        System.out.println("Directory " + outputDir + " is created.");
      }
      if (!dir.canWrite()) {
        throw new Exception("Directory " + outputDir + " is not writeable.");
      }
      File zipFile = new File(dir, zipName);
      if (zipFile.exists()) {
        zipFile.delete();
        System.out.println("File " + zipName + 
            " already exists, old version is deleted.");
      }
      File recordsFile = new File(dir, "records.dat");
      if (recordsFile.exists())
        recordsFile.delete();
      File infoFile = new File(dir, "export.info");
      if (infoFile.exists())
        infoFile.delete();

      RandomAccessFile out2 = null;
      out2 = new RandomAccessFile(recordsFile, "rw");
      Connection conn = DriverManager.getConnection("jdbc:mysql://" + address
          + ":" + port + "/" + database, username, password);
      List<Integer> docIDs = new ArrayList<Integer>();
      if (incremental) {
        Statement stmt2 = conn.createStatement();
        stmt2.executeUpdate("CREATE TABLE IF NOT EXISTS export_history (export_date DATE NOT NULL, export_type CHAR(1) NOT NULL, PRIMARY KEY(export_date))");
        ResultSet rset2 = stmt2.executeQuery(
            "SELECT max(export_date) FROM export_history");
        if (rset2.next()) {
          startDate = rset2.getDate(1);
          if (startDate != null)
            System.out.println("Incremental export from date: " + sdf.format(startDate));
          else {
            incremental = false;
            System.out.println("No previous export, doing full export.");
          }
        } else {
          incremental = false;
          System.out.println("No previous export, doing full export.");
        }
        rset2.close();
        stmt2.close();
      }

      PreparedStatement stmt = null;
      ResultSet rset = null;
      if (incremental) {
        stmt = conn.prepareStatement("SELECT record_id FROM Records WHERE date_created>=? OR date_modified>=?");
        stmt.setDate(1, new java.sql.Date(startDate.getTime()));
        stmt.setDate(2, new java.sql.Date(startDate.getTime()));
        rset = stmt.executeQuery();
      } else {
        stmt = conn.prepareStatement("SELECT record_id FROM Records");
        rset = stmt.executeQuery();
      }
      while (rset.next())
        docIDs.add(rset.getInt(1));
      rset.close();
      stmt.close();
      
      try {
        PreparedStatement stmt1 = null;
        stmt1 = conn.prepareStatement(
            "INSERT INTO export_history (export_date, export_type) VALUES (?,?)");
        stmt1.setDate(1, new java.sql.Date(today.getTime()));
        stmt1.setString(2, incremental ? "I" : "F");
        stmt1.executeUpdate();
        stmt1.close();
      } catch (Exception e) {
        System.out.println("Export for today already created!");
      }
      
      System.out.println("Found " + docIDs.size() + " records in the database.");
      System.out.println("Dumping to " + zipFile);
      DBStorage storage = new DBStorage();
      int i = 0;
      for (int id: docIDs) {
        Record rec = storage.get(conn, id);
        rec.pack();
        if (rec.getFieldCount() > 0) {
          rec = PrimerakSerializer.metapodaciUPolje000(rec); 
          out2.writeBytes(UnimarcSerializer.toUNIMARC(0, rec, true).replace('\n', '\t'));
          out2.writeBytes("\n");
          if (i > 0 && i % 1000 == 0)
            System.out.println(Integer.toString(i) + " records dumped.");
          i++;
        }
      }
      System.out.println("Total " + Integer.toString(i) + " records dumped.");
      conn.close();
      out2.close();

      ExportInfo exportInfo = new ExportInfo();
      exportInfo.setExportDate(today);
      exportInfo.setExportType(incremental ? ExportInfo.TYPE_INCREMENTAL : ExportInfo.TYPE_FULL);
      if (incremental)
        exportInfo.setStartDate(startDate);
      exportInfo.setRecordCount(i);
      BufferedWriter info = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(infoFile), "UTF8"));
      info.write(exportInfo.toString());
      info.close();
      
      ZipUtils.zip(zipFile, new File[] { recordsFile, infoFile} );
      recordsFile.delete();
      infoFile.delete();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
