package bisis.records.serializers;

import bisis.records.Record;


public interface RecordListener {
  public void handleRecord(Record rec);
}
