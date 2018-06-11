package bisis.export;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ExportInfo {
   
  public static final int TYPE_FULL         = 1;
  public static final int TYPE_INCREMENTAL  = 2;
  
  private Date exportDate;
  private Date startDate;
  private int exportType;
  private int recordCount;

  public ExportInfo(Date exportDate, Date startDate, int exportType, 
      int recordCount) {
    this.exportDate = exportDate;
    this.startDate = startDate;
    this.exportType = exportType;
    this.recordCount = recordCount;
  }

  public ExportInfo() {
  }
  
  public ExportInfo(String fileContents) {
    String[] lines = fileContents.split("\n");
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].trim().length() == 0)
        continue;
      String[] parts = lines[i].split("=");
      if (parts.length < 2)
        continue;
      String property = parts[0].trim();
      String value = parts[1].trim();
      if ("export.date".equals(property)) {
        try {
          exportDate = sdf.parse(value);
        } catch (Exception ex) {
          continue;
        }
      } else if ("export.type".equals(property)) {
        if ("full".equals(value))
          exportType = TYPE_FULL;
        else if ("incremental".equals(value))
          exportType = TYPE_INCREMENTAL;
      }
      else if ("start.date".equals(property)) {
        try {
          startDate = sdf.parse(value);
        } catch (Exception ex) {
          continue;
        }
      } else if ("record.count".equals(property)) {
        try {
          recordCount = Integer.parseInt(value);
        } catch (Exception ex) {
          continue;
        }
      }
      
    }
  }

  public Date getExportDate() {
    return exportDate;
  }

  public void setExportDate(Date exportDate) {
    this.exportDate = exportDate;
  }

  public int getExportType() {
    return exportType;
  }

  public void setExportType(int exportType) {
    this.exportType = exportType;
  }
  
  public Date getStartDate() {
    return startDate;
  }

  public void setStartDate(Date startDate) {
    this.startDate = startDate;
  }

  public int getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  public String toString() {
    String retVal = "export.date=" + sdf.format(exportDate) + "\n";
    retVal += "export.type=" + 
      (exportType == TYPE_FULL ? "full" : "incremental") + "\n";
    if (exportType == TYPE_INCREMENTAL && startDate != null)
      retVal += "start.date=" + sdf.format(startDate) + "\n";
    retVal += "record.count=" + recordCount + "\n";
    return retVal;
  }
  
  private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
}
