// @author    Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
// @date      2023-04-10
// @version   1.0 
//
// Description 
// This API is to add contract section grade to EXTCSG
// Transaction AddSectionGrade
// AFMNI-7/Alias Replacement
// https://leanswift.atlassian.net/browse/AFMI-7

/**
 * IN
 * @param: DSID - Section ID
 * @param: GRAD - Grade Code
 * 
*/

/**
 * OUT
 * @return: CONO - Company Number
 * @return: DIVI - Division
 * @return: DSID - Section ID
 * @return: SGID - Grade ID
 * 
*/


public class AddSectionGrade extends ExtendM3Transaction {
  private final MIAPI mi 
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final LoggerAPI logger
  private final UtilityAPI utility
  
  Integer inCONO
  String inDIVI
  int inSGID
  boolean numberFound
  Integer lastNumber
  
  // Constructor 
  public AddSectionGrade(MIAPI mi, DatabaseAPI database, MICallerAPI miCaller, ProgramAPI program, LoggerAPI logger, UtilityAPI utility) {
     this.mi = mi
     this.database = database
     this.miCaller = miCaller
     this.program = program
     this.logger = logger
     this.utility = utility
  } 
    
  public void main() {       
     // Set Company Number
     inCONO = mi.in.get("CONO")      
     if (inCONO == null || inCONO == 0) {
        inCONO = program.LDAZD.CONO as Integer
     } 

     // Set Division
     inDIVI = mi.inData.get("DIVI").trim()
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
     if (mi.inData.get("GRAD") != null && mi.inData.get("GRAD") != "") {
        inGRAD = mi.inData.get("GRAD").trim() 
     } else {
        inGRAD = ""        
     }

     // Validate Contract Grade Section record
     Optional<DBContainer> EXTCSG = findEXTCSG(inCONO, inDIVI, inDSID, inGRAD)
     if(EXTCSG.isPresent()){
        mi.error("Contract Section Grade already exists")   
        return             
     } else {
        findLastNumber()
        inSGID = lastNumber + 1
        // Write record 
        addEXTCSGRecord(inCONO, inDIVI, inSGID, inDSID, inGRAD)          
     }  

     mi.outData.put("CONO", String.valueOf(inCONO)) 
     mi.outData.put("DIVI", inDIVI) 
     mi.outData.put("SGID", String.valueOf(inSGID))      
     mi.write()

  }
  

   //******************************************************************** 
   // Find last id number used
   //********************************************************************  
   void findLastNumber(){   
     
     numberFound = false
     lastNumber = 0

     ExpressionFactory expression = database.getExpressionFactory("EXTCSG")
     
     expression = expression.eq("EXCONO", String.valueOf(inCONO)).and(expression.eq("EXDIVI", inDIVI))
     
     // Get Last Number
     DBAction actionline = database.table("EXTCSG")
     .index("10")
     .matching(expression)
     .selection("EXSGID")
     .reverse()
     .build()

     DBContainer line = actionline.getContainer() 
     
     line.set("EXCONO", inCONO)     
     
     int pageSize = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords()        
     actionline.readAll(line, 1, pageSize, releasedLineProcessor)   
   
   } 

    
  //******************************************************************** 
  // List Last Number
  //********************************************************************  
  Closure<?> releasedLineProcessor = { DBContainer line -> 

      // Output
      if (!lastNumber) {
        lastNumber = line.get("EXSGID") 
        numberFound = true
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
  // Add EXTCSG record 
  //********************************************************************     
  void addEXTCSGRecord(int CONO, String DIVI, int SGID, int DSID, String GRAD){     
       DBAction action = database.table("EXTCSG").index("00").build()
       DBContainer EXTCSG = action.createContainer()
       EXTCSG.set("EXCONO", CONO)
       EXTCSG.set("EXDIVI", DIVI)
       EXTCSG.set("EXSGID", SGID)
       EXTCSG.set("EXDSID", DSID)
       EXTCSG.set("EXGRAD", GRAD)   
       EXTCSG.set("EXCHID", program.getUser())
       EXTCSG.set("EXCHNO", 1) 
       int regdate = utility.call("DateUtil", "currentDateY8AsInt")
       int regtime = utility.call("DateUtil", "currentTimeAsInt")                    
       EXTCSG.set("EXRGDT", regdate) 
       EXTCSG.set("EXLMDT", regdate) 
       EXTCSG.set("EXRGTM", regtime)
       action.insert(EXTCSG)         
 } 

     
} 

