// @author    Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
// @date      2023-09-25
// @version   1.0 
//
// Description 
// This API is to get a contract brand from EXTCPI
// Transaction GetContrPaymt
// AFMNI-7/Alias Replacement
// https://leanswift.atlassian.net/browse/AFMI-7

/**
 * IN
 * @param: CONO - Company Number
 * @param: DIVI - Division
 * @param: CTNO - Contract Number
 * @param: RVID - Revision ID
 * @param: DLFY - Deliver From Yard
 * @param: DLTY - Deliver To Yard
*/

/**
 * OUT
 * @return: CONO - Company Number
 * @return: DIVI - Division
 * @return: CTNO - Contract Number
 * @return: RVID - Revision ID
 * @return: DLFY - Deliver From Yard
 * @return: DLTY - Deliver To Yard
 * @return: TRRA - Trip Rate
 * @return: MTRA - Minimum Amount
 * 
*/



public class GetContrPaymt extends ExtendM3Transaction {
  private final MIAPI mi 
  private final DatabaseAPI database 
  private final ProgramAPI program
  private final LoggerAPI logger
  
  Integer inCONO
  String inDIVI
  int inPINO
  int inCTNO
  String inRVID
  String inDLNO
  
  // Constructor 
  public GetContrPaymt(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
     this.mi = mi
     this.database = database  
     this.program = program
     this.logger = logger
  } 
    
  public void main() { 
     // Set Company Number
     inCONO = mi.in.get("CONO")      
     if (inCONO == null || inCONO == 0) {
        inCONO = program.LDAZD.CONO as Integer
     } 

     // Set Division
     inDIVI = mi.in.get("DIVI")
     if (inDIVI == null || inDIVI == "") {
        inDIVI = program.LDAZD.DIVI
     }

     // Payment Number
     if (mi.in.get("PINO") != null) {
        inPINO = mi.in.get("PINO") 
     } else {
        inPINO = 0      
     }

     // Contract Number
     if (mi.in.get("CTNO") != null) {
        inCTNO = mi.in.get("CTNO") 
     } else {
        inCTNO = 0      
     }
     
     // Revision ID
     if (mi.in.get("RVID") != null && mi.in.get("RVID") != "") {
        inRVID = mi.inData.get("RVID").trim() 
     } else {
        inRVID = ""      
     }

     // Delivery Number
     if (mi.in.get("DLNO") != null) {
        inDLNO = mi.in.get("DLNO") 
     } else {
        inDLNO = 0      
     }

     // Get record
     getRecord()
  }
 
 //******************************************************************** 
 //Get EXTCPI record
 //********************************************************************     
  void getRecord(){      
     DBAction action = database.table("EXTCPI").index("00").selectAllFields().build()
     DBContainer EXTCPI = action.getContainer()
      
     // Key value for read
     EXTCPI.set("EXCONO", inCONO)
     EXTCPI.set("EXDIVI", inDIVI)
     EXTCPI.set("EXPINO", inPINO)
     EXTCPI.set("EXCTNO", inCTNO)
     EXTCPI.set("EXRVID", inRVID)
     EXTCPI.set("EXDLNO", inDLNO)
     
    // Read  
    if (action.read(EXTCPI)) {       
      mi.outData.put("CONO", EXTCPI.get("EXCONO").toString())
      mi.outData.put("DIVI", EXTCPI.getString("EXDIVI"))
      mi.outData.put("PINO", EXTCPI.get("EXPINO").toString())
      mi.outData.put("CTNO", EXTCPI.get("EXCTNO").toString())
      mi.outData.put("RVID", EXTCPI.getString("EXRVID"))
      mi.outData.put("DLNO", EXTCPI.get("EXDLNO").toString())
      mi.outData.put("SUNO", EXTCPI.getString("EXSUNO"))
      mi.outData.put("ITNO", EXTCPI.getString("EXITNO"))
      mi.outData.put("PODT", EXTCPI.get("EXPODT").toString())
      mi.outData.put("DUDT", EXTCPI.get("EXDUDT").toString())
      mi.outData.put("BADT", EXTCPI.get("EXBADT").toString())
      mi.outData.put("POTO", EXTCPI.getString("EXPOTO"))
      mi.outData.put("NEBF", EXTCPI.getDouble("EXNEBF").toString())
      mi.outData.put("PIAM", EXTCPI.getDouble("EXPIAM").toString())
      mi.outData.put("APDT", EXTCPI.get("EXAPDT").toString())
      mi.outData.put("PIAU", EXTCPI.getString("EXPIAU"))
      mi.outData.put("STAT", EXTCPI.get("EXSTAT").toString())
      mi.outData.put("TREF", EXTCPI.getString("EXTREF"))
      mi.write() 
    } else {
      mi.error("No record found")   
      return 
    }
  }  
}