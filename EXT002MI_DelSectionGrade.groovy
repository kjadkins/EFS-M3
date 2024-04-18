// @author    Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
// @date      2023-04-10
// @version   1.0 
//
// Description 
// This API is to delete a contract section grade from EXTCSG
// Transaction DelSectionGrade
// AFMNI-7/Alias Replacement
// https://leanswift.atlassian.net/browse/AFMI-7

/**
 * IN
 * 
 * @param: CONO - Company Number
 * @param: DIVI - Division
 * @param: DSID - Section ID
 * @param: GRAD - Grade Code
 * 
*/


 public class DelSectionGrade extends ExtendM3Transaction {
    private final MIAPI mi 
    private final DatabaseAPI database 
    private final ProgramAPI program
    private final LoggerAPI logger
    
    Integer inCONO
    String inDIVI
  
    // Constructor 
    public DelSectionGrade(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
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
       
       // Grade Code
       String inGRAD      
       if (mi.in.get("GRAD") != null && mi.in.get("GRAD") != "") {
          inGRAD = mi.in.get("GRAD") 
       } else {
          inGRAD = ""     
       }
  
       // Validate contract section grade record
       Optional<DBContainer> EXTCSG = findEXTCSG(inCONO, inDIVI, inDSID, inGRAD)
       if(!EXTCSG.isPresent()){
          mi.error("Contract Section Grade doesn't exist")   
          return             
       } else {
          // Delete records 
          deleteEXTCSGRecord(inCONO, inDIVI, inDSID, inGRAD) 
       } 
       
    }
  
  
    //******************************************************************** 
    // Get EXTCSG record
    //******************************************************************** 
    private Optional<DBContainer> findEXTCSG(int CONO, String DIVI, int DSID, String GRAD){  
       DBAction query = database.table("EXTCSG").index("00").build()
       DBContainer EXTCSG = query.getContainer()
       EXTCSG.set("EXCONO", CONO)
       EXTCSG.set("EXDIVI", DIVI)
       EXTCSG.set("EXDSID", DSID)
       EXTCSG.set("EXGRAD", GRAD)
       
       if(query.read(EXTCSG))  { 
         return Optional.of(EXTCSG)
       } 
    
       return Optional.empty()
    }
    
  
    //******************************************************************** 
    // Delete record from EXTCSG
    //******************************************************************** 
    void deleteEXTCSGRecord(int CONO, String DIVI, int DSID, String GRAD){ 
       DBAction action = database.table("EXTCSG").index("00").build()
       DBContainer EXTCSG = action.getContainer()
       EXTCSG.set("EXCONO", CONO)
       EXTCSG.set("EXDIVI", DIVI)
       EXTCSG.set("EXDSID", DSID)
       EXTCSG.set("EXGRAD", GRAD)
  
       action.readLock(EXTCSG, deleterCallbackEXTCSG)
    }
      
    Closure<?> deleterCallbackEXTCSG = { LockedResult lockedResult ->  
       lockedResult.delete()
    }
    

 }