package bisis.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ZipUtils {
  public static void unzip(File zipFile) {
    File dir = zipFile.getParentFile();
    byte[] buffer = new byte[65536];
    try {
      ZipFile zip = new ZipFile(zipFile);
      Enumeration<? extends ZipEntry> e = zip.entries();
      while (e.hasMoreElements()) {
        ZipEntry entry = (ZipEntry)e.nextElement();
        BufferedInputStream bis = new BufferedInputStream(
            zip.getInputStream(entry));
        BufferedOutputStream bos = new BufferedOutputStream(
            new FileOutputStream(new File(dir, entry.getName()))); 
        int bytesRead = 0;
        do {
          bytesRead = bis.read(buffer);
          bos.write(buffer, 0, bytesRead);
        } while (bytesRead == buffer.length);
        bos.close();
        bis.close();
      }
      zip.close();
    } catch (Exception ex) {
      log.fatal(ex);
    }
  }
  
  public static void zip(File zipFile, File[] files) {
    byte[] buffer = new byte[65536];
    try {
      ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipFile));
      for (int i = 0; i < files.length; i++) {
        zipOut.putNextEntry(new ZipEntry(files[i].getName()));
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(files[i]));
        int bytesRead = 0;
        do {
          bytesRead = bis.read(buffer);
          if (bytesRead > 0)
            zipOut.write(buffer, 0, bytesRead);
        } while (bytesRead == buffer.length);
        bis.close();
      }
      zipOut.close();
    } catch (Exception ex) {
      log.fatal(ex);
    }
  }
  
  private static Log log = LogFactory.getLog(ZipUtils.class.getName());
}
