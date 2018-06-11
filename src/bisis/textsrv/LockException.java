package bisis.textsrv;

/**
 * 
 * @author mbranko@uns.ns.ac.yu
 */
@SuppressWarnings("serial")
public class LockException extends Exception {

  public LockException(String inUseBy) {
    this.inUseBy = inUseBy;
  }
  
  public String getInUseBy() {
    return inUseBy;
  }
  
  public String getMessage() {
    return "The record is locked by " + inUseBy;
  }
  
  private String inUseBy;
}
