// @author    Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
// @date      2023-04-10
// @version   1.0 
//
// Description 
// This API is to add contract section exceptions to EXTCSE
// Transaction AddSectionExcep
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

/**
 * OUT
 * @param: CONO - Company Number
 * @param: DIVI - Division
 * @param: SEID - Exception ID
 * 
*/


public class AddSectionExcep extends ExtendM3Transaction {
  private final MIAPI mi 
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final LoggerAPI logger
  private final UtilityAPI utility
  
  Integer inCONO
  String inDIVI
  int inSEID
  boolean numberFound
  Integer lastNumber
  
  // Constructor 
  public AddSectionExcep(MIAPI mi, DatabaseAPI database, MICallerAPI miCaller, ProgramAPI program, LoggerAPI logger, UtilityAPI utility) {
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
        inECOD = mi.inData.get("ECOD").trim() 
     } else {
        inECOD = ""        
     }

     // Validate Contract Grade Section record
     Optional<DBContainer> EXTCSE = findEXTCSE(inCONO, inDIVI, inDSID, inECOD)
     if(EXTCSE.isPresent()){
        mi.error("Contract Section Exception already exists")   
        return             
     } else {
        findLastNumber()
        inSEID = lastNumber + 1
        // Write record 
        addEXTCSERecord(inCONO, inDIVI, inSEID, inDSID, inECOD)          
     }  

     mi.outData.put("CONO", String.valueOf(inCONO)) 
     mi.outData.put("DIVI", inDIVI) 
     mi.outData.put("SEID", String.valueOf(inSEID))    
     mi.write()

  }

   //******************************************************************** 
   // Find last id number used
   //********************************************************************  
   void findLastNumber(){   
     
     numberFound = false
     lastNumber = 0

     ExpressionFactory expression = database.getExpressionFactory("EXTCSE")
     
     expression = expression.eq("EXCONO", String.valueOf(inCONO)).and(expression.eq("EXDIVI", inDIVI))
     
     // Get Last Number
     DBAction actionline = database.table("EXTCSE")
     .index("10")
     .matching(expression)
     .selection("EXSEID")
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
        lastNumber = line.get("EXSEID") 
        numberFound = true
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
  // Add EXTCSE record 
  //********************************************************************     
  void addEXTCSERecord(int CONO, String DIVI, int SEID, int DSID, String ECOD){     
       DBAction action = database.table("EXTCSE").index("00").build()
       DBContainer EXTCSE = action.createContainer()
       EXTCSE.set("EXCONO", CONO)
       EXTCSE.set("EXDIVI", DIVI)
       EXTCSE.set("EXSEID", SEID)
       EXTCSE.set("EXDSID", DSID)
       EXTCSE.set("EXECOD", ECOD)   
       EXTCSE.set("EXCHID", program.getUser())
       EXTCSE.set("EXCHNO", 1) 
       int regdate = utility.call("DateUtil", "currentDateY8AsInt")
       int regtime = utility.call("DateUtil", "currentTimeAsInt")                    
       EXTCSE.set("EXRGDT", regdate) 
       EXTCSE.set("EXLMDT", regdate) 
       EXTCSE.set("EXRGTM", regtime)
       action.insert(EXTCSE)         
 } 

     
} 

