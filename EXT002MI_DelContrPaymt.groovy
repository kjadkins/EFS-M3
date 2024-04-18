// @author    Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
// @date      2023-09-25
// @version   1.0 
//
// Description 
// This API is to delete contract payment from EXTCPI
// Transaction DelContrPaymt
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
 * 
*/

 public class DelContrPaymt extends ExtendM3Transaction {
    private final MIAPI mi 
    private final DatabaseAPI database 
    private final ProgramAPI program
    private final LoggerAPI logger
  
    Integer inCONO
    String inDIVI
    int inPINO
    int inCTNO  
    String inRVID
    int inDLNO

  // Constructor 
  public DelContrPaymt(MIAPI mi, DatabaseAPI database,ProgramAPI program, LoggerAPI logger) {
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


     // Validate contract payment
     Optional<DBContainer> EXTCPI = findEXTCPI(inCONO, inDIVI, inPINO, inCTNO, inRVID, inDLNO)
     if(!EXTCPI.isPresent()){
        mi.error("Contract Payment doesn't exist")   
        return             
     } else {
        // Delete records 
        deleteEXTCPIRecord() 
     } 
     
  }
 

  //******************************************************************** 
  // Get EXTCPI record
  //******************************************************************** 
  private Optional<DBContainer> findEXTCPI(int CONO, String DIVI, int PINO, int CTNO, String RVID, int DLNO){  
     DBAction query = database.table("EXTCPI").index("00").build()
     DBContainer EXTCPI = query.getContainer()
     EXTCPI.set("EXCONO", CONO)
     EXTCPI.set("EXDIVI", DIVI)
     EXTCPI.set("EXPINO", PINO)
     EXTCPI.set("EXCTNO", CTNO)
     EXTCPI.set("EXRVID", RVID)
     EXTCPI.set("EXDLNO", DLNO)
     
     if(query.read(EXTCPI))  { 
       return Optional.of(EXTCPI)
     } 
  
     return Optional.empty()
  }
  

  //******************************************************************** 
  // Delete record in EXTCPI
  //******************************************************************** 
  void deleteEXTCPIRecord(){ 
     DBAction action = database.table("EXTCPI").index("00").build()
     DBContainer EXTCPI = action.getContainer()
     EXTCPI.set("EXCONO", inCONO) 
     EXTCPI.set("EXDIVI", inDIVI) 
     EXTCPI.set("EXPINO", inPINO) 
     EXTCPI.set("EXCTNO", inCTNO)
     EXTCPI.set("EXRVID", inRVID)
     EXTCPI.set("EXDLNO", inDLNO)
     action.readLock(EXTCPI, deleterCallbackEXTCPI)
  }
    
  Closure<?> deleterCallbackEXTCPI = { LockedResult lockedResult ->  
     lockedResult.delete()
  }
  
  
 }