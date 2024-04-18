// @author    Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
// @date      2023-04-10
// @version   1.0 
//
// Description 
// This API is to delete a contract section exception from EXTCSE
// Transaction DelSectionExcep
// AFMNI-7/Alias Replacement
// https://leanswift.atlassian.net/browse/AFMI-7

/**
 * IN
 * @param: CONO - Company Number
 * @param: DIVI - Division
 * @param: DSID - Section ID
 * @param: ECOD - Exception Code
 * 
*/


 public class DelSectionExcep extends ExtendM3Transaction {
    private final MIAPI mi 
    private final DatabaseAPI database 
    private final ProgramAPI program
    private final LoggerAPI logger
    
    Integer inCONO
    String inDIVI
  
    // Constructor 
    public DelSectionExcep(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
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
  
       // Section ID
       int inDSID      
       if (mi.in.get("DSID") != null) {
          inDSID = mi.in.get("DSID") 
       } else {
          inDSID = 0     
       }
       
       // Exception Code
       String inECOD      
       if (mi.in.get("ECOD") != null && mi.in.get("ECOD") != "") {
          inECOD = mi.in.get("ECOD") 
       } else {
          inECOD = ""     
       }
  
       // Validate contract section exception record
       Optional<DBContainer> EXTCSE = findEXTCSE(inCONO, inDIVI, inDSID, inECOD)
       if(!EXTCSE.isPresent()){
          mi.error("Contract Section Exception doesn't exist")   
          return             
       } else {
          // Delete records 
          deleteEXTCSERecord(inCONO, inDIVI, inDSID, inECOD) 
       } 
       
    }
  
  
  
    //******************************************************************** 
    // Get EXTCSE record
    //******************************************************************** 
    private Optional<DBContainer> findEXTCSE(int CONO, String DIVI, int DSID, String ECOD){  
       DBAction query = database.table("EXTCSE").index("00").build()
       DBContainer EXTCSE = query.getContainer()
       EXTCSE.set("EXCONO", CONO)
       EXTCSE.set("EXDIVI", DIVI)
       EXTCSE.set("EXDSID", DSID)
       EXTCSE.set("EXECOD", ECOD)
       
       if(query.read(EXTCSE))  { 
         return Optional.of(EXTCSE)
       } 
    
       return Optional.empty()
    }
    
  
    //******************************************************************** 
    // Delete record from EXTCSE
    //******************************************************************** 
    void deleteEXTCSERecord(int CONO, String DIVI, int DSID, String ECOD){ 
       DBAction action = database.table("EXTCSE").index("00").build()
       DBContainer EXTCSE = action.getContainer()
       EXTCSE.set("EXCONO", CONO)
       EXTCSE.set("EXDIVI", DIVI)
       EXTCSE.set("EXDSID", DSID)
       EXTCSE.set("EXECOD", ECOD)
  
       action.readLock(EXTCSE, deleterCallbackEXTCSE)
    }
      
    Closure<?> deleterCallbackEXTCSE = { LockedResult lockedResult ->  
       lockedResult.delete()
    }
    

 }