package bisis.format;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class HoldingsDataCodersJdbc{
	
  private static Log log = LogFactory.getLog(HoldingsDataCodersJdbc.class.getName());
	
	static private List<UItem> sif_Odeljenje = new ArrayList<UItem>(); 
	static private List<UItem> sif_Format = new ArrayList<UItem>();
	static private List<UItem> sif_Status = new ArrayList<UItem>();
	static private List<UItem> sif_Povez = new ArrayList<UItem>();
	static private List<UItem> sif_Podlokacija = new ArrayList<UItem>();
	static private List<UItem> sif_NacinNabavke = new ArrayList<UItem>();
	static private List<UItem> sif_InternaOznaka = new ArrayList<UItem>();
	static private List<UItem> sif_InvKnj = new ArrayList<UItem>();
	static private List<UItem> sif_Dostupnost = new ArrayList<UItem>();
  
  //zaduzivost statusa
	static private HashMap<String, Integer> zaduzivostStatusa = new HashMap<String,Integer>(); 
  
  
	public static final int ODELJENJE_CODER = 				0;
	public static final int FORMAT_CODER =    				1;
	public static final int STATUS_CODER =    				2;
	public static final int POVEZ_CODER =     				3;
	public static final int PODLOKACIJA_CODER =   		4;
	public static final int NACINNABAVKE_CODER =  		5;
	public static final int INTERNAOZNAKA_CODER =   	6;
	public static final int INVENTARNAKNJIGA_CODER =	7;
	public static final int DOSTUPNOST_CODER =     		8;
  
	
/*	static{		
		init();
	}*/
  
  public static void loadData(Connection conn){
  	 try {
       Statement stmt = conn.createStatement();
       ResultSet rs = stmt.executeQuery("SELECT * FROM Odeljenje;");
       sif_Odeljenje = createList(rs);     
       rs = stmt.executeQuery("SELECT * FROM SigFormat;");
       sif_Format = createList(rs);
       rs = stmt.executeQuery("SELECT * FROM Status_Primerka");
       sif_Status = createList(rs);
       rs.beforeFirst();       
       loadZaduzivost(rs);       
       rs = stmt.executeQuery("SELECT * FROM Povez");
       sif_Povez = createList(rs);
       rs = stmt.executeQuery("SELECT * FROM Podlokacija");
       sif_Podlokacija = createList(rs);
       rs = stmt.executeQuery("SELECT * FROM Nacin_nabavke");
       sif_NacinNabavke = createList(rs);
       rs = stmt.executeQuery("SELECT * FROM Interna_oznaka");
       sif_InternaOznaka = createList(rs);
       rs = stmt.executeQuery("SELECT * FROM Invknj");
       sif_InvKnj = createList(rs);     
       rs = stmt.executeQuery("SELECT * FROM Dostupnost");
       sif_Dostupnost = createList(rs);  
       rs.close();
       stmt.close();
     } catch (SQLException ex) {      
       log.fatal(ex);
     }   
  	
  }
  
  
	
/*	public static void init(){
    Connection conn = BisisApp.getConnection();
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT * FROM Odeljenje;");
      sif_Odeljenje = createList(rs);     
      rs = stmt.executeQuery("SELECT * FROM SigFormat;");
      sif_Format = createList(rs);
      rs = stmt.executeQuery("SELECT * FROM Status_Primerka");
      sif_Status = createList(rs);
      rs = stmt.executeQuery("SELECT * FROM Povez");
      sif_Povez = createList(rs);
      rs = stmt.executeQuery("SELECT * FROM Podlokacija");
      sif_Podlokacija = createList(rs);
      rs = stmt.executeQuery("SELECT * FROM Nacin_nabavke");
      sif_NacinNabavke = createList(rs);
      rs = stmt.executeQuery("SELECT * FROM Interna_oznaka");
      sif_InternaOznaka = createList(rs);
      rs = stmt.executeQuery("SELECT * FROM Invknj");
      sif_InvKnj = createList(rs);     
      rs = stmt.executeQuery("SELECT * FROM Dostupnost");
      sif_Dostupnost = createList(rs);     
    } catch (SQLException ex) {      
      log.fatal(ex);
    }   
  }*/
	
	private static List<UItem> createList(ResultSet rs){
		List<UItem> list = new ArrayList<UItem>();
		UItem ui;
		try {
			while(rs.next()){
				ui = new UItem(rs.getString(1),rs.getString(2));				
				list.add(ui);
			}
		} catch (SQLException ex) {			
			log.fatal(ex);
		}	
		return list;
	}
	
	
	/*
	 * vraca vrednost za prosledjeni kod iz prosledjene liste
	 * ili null ako sifre nema u listi
	 */
	private static String getValueFromList(List<UItem> sif_list,String code){
		for(UItem ui:sif_list){
			if(ui.getCode().equals(code)) return ui.getValue();
		}
		return null;
	}
	
	private static boolean containsCode(String code,List<UItem> list){
		for(int i=0;i<list.size();i++){
			if(list.get(i).getCode().equals(code)) return true;
		}
		return false;
	}
	
	private static void loadZaduzivost(ResultSet rs){
		zaduzivostStatusa.clear();		
		try {
			while(rs.next()){
				zaduzivostStatusa.put(rs.getString(1).toLowerCase(),rs.getInt(3));
			}
			
		} catch (SQLException ex) {			
			log.fatal(ex);
		}	
	}
	
	
	/*
	 * vraca vrednost za prosledjenu sifru i to iz bilo kog sifarnika
	 * u kom sifarniku ce se traziti sifra zavisi od prvog parametra
	 * u kom se prosledjuje jedna od konstanti definisanih u ovoj klasi 
	 * 
	 */
	public static String getValue(int coder_ref, String code){
		switch(coder_ref){
			case ODELJENJE_CODER: return getValueFromList(sif_Odeljenje, code);
			case FORMAT_CODER: return getValueFromList(sif_Format, code);
			case STATUS_CODER: return getValueFromList(sif_Status, code);
			case POVEZ_CODER: return getValueFromList(sif_Povez, code);
			case PODLOKACIJA_CODER: return getValueFromList(sif_Podlokacija, code);
			case NACINNABAVKE_CODER: return getValueFromList(sif_NacinNabavke, code);
			case INTERNAOZNAKA_CODER: return getValueFromList(sif_InternaOznaka, code);
			case INVENTARNAKNJIGA_CODER: return getValueFromList(sif_InvKnj, code);
			case DOSTUPNOST_CODER: return getValueFromList(sif_Dostupnost, code);		
		}		
		return null;
		
	}
	
	public static List<UItem> getCoder(int coder_ref){
		switch(coder_ref){
			case ODELJENJE_CODER: return sif_Odeljenje;
			case FORMAT_CODER: return sif_Format;
			case STATUS_CODER: return sif_Status;
			case POVEZ_CODER: return sif_Povez;
			case PODLOKACIJA_CODER: return sif_Podlokacija;
			case NACINNABAVKE_CODER: return sif_NacinNabavke;
			case INTERNAOZNAKA_CODER: return sif_InternaOznaka;
			case INVENTARNAKNJIGA_CODER: return sif_InvKnj;
			case DOSTUPNOST_CODER: return sif_Dostupnost;		
		}		
		return null;	
	}
	
	public static int getZaduzivostStatusa(String code){
		return zaduzivostStatusa.get(code.toLowerCase());
	}
	
	public static boolean isValidOdeljenje(String code){
    if(sif_Odeljenje.size()>0)
      return containsCode(code,sif_Odeljenje);
    else return true;
	}
	
	public static boolean isValidFormat(String code){
		return containsCode(code,sif_Format);
	}
	
	public static boolean isValidStatus(String code){
		return containsCode(code,sif_Status);
	}
	
	public static boolean isValidPovez(String code){
		return containsCode(code,sif_Povez);
	}
	
	public static boolean isValidPodlokacija(String code){
		return containsCode(code,sif_Podlokacija);
	}
	
	public static boolean isValidNacinNabavke(String code){
		return containsCode(code,sif_NacinNabavke);
	}
	
	public static boolean isValidInternaOznaka(String code){
		return containsCode(code,sif_InternaOznaka);
	}
  
	public static boolean isValidDostupnost(String code){
		return containsCode(code,sif_Dostupnost);
	}
	
}
