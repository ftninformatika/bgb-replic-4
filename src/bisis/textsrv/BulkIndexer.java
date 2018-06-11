package bisis.textsrv;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexWriter;

import bisis.records.Record;

public class BulkIndexer extends Indexer {

  public BulkIndexer() {
    super();
  }
  
  public BulkIndexer(String indexPath) {
    super(indexPath);
  }

  public boolean add(Record rec) {
    try {
      getIndexWriter().addDocument(getDocument(rec));
    } catch (IOException ex) {
      log.fatal(ex);
      return false;
    }
    return true;
  }
  
  public void close() {
    try {
      writer.close();
    } catch (Exception ex) {
      log.fatal(ex);
    }
  }
  
  @Override
  protected IndexWriter getIndexWriter() {
    if (++useCount % 1000 == 0) {
      close();
      writer = null;
    }
    if (writer == null) {
      try {
        boolean createIndex = true;
        File testIndexPath = new File(indexPath);
        if (!testIndexPath.exists())
          testIndexPath.mkdirs();
        if (testIndexPath.isDirectory()) {
          if (testIndexPath.list().length > 0)
            createIndex = false;
          writer = new IndexWriter(indexPath, new WhitespaceAnalyzer(), createIndex);
        }
      } catch (Exception ex) {
        log.fatal(ex);
      }
    }
    return writer;
  }
  
  private int useCount = 0;
  private IndexWriter writer = null;
  private static Log log = LogFactory.getLog(BulkIndexer.class.getName());
}
