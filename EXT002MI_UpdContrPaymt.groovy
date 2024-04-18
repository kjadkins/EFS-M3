// @author    Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
// @date      2023-09-25
// @version   1.0 
//
// Description 
// This API is to update a contract payment in EXTCPI
// Transaction UpdContrPaymt
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
 * @param: TRRA - Trip Rate
 * @param: MTRA - Minimum Amount
 * 
*/



public class UpdContrPaymt extends ExtendM3Transaction {
  private final MIAPI mi 
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private final LoggerAPI logger
  
  Integer inCONO
  String inDIVI
  int inPINO
  String inRVID
  int inCTNO
  int inDLNO
  String inSUNO
  String inITNO
  int inPODT
  int inDUDT
  int inBADT
  String inPOTO  
  double inNEBF
  double inPIAM
  int inAPDT
  String inPIAU
  int inSTAT
  String inTREF
  
  // Constructor 
  public UpdContrPaymt(MIAPI mi, DatabaseAPI database, MICallerAPI miCaller, ProgramAPI program, UtilityAPI utility, LoggerAPI logger) {
     this.mi = mi
     this.database = database
     this.miCaller = miCaller
     this.program = program
     this.utility = utility
     this.logger = logger     
  } 
    
  public void main() {       
     // Set Company Number
     if (inCONO == null || inCONO == 0) {
        inCONO = program.LDAZD.CONO as Integer
     } 

     // Set Division
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

     // Supplier
     if (mi.in.get("SUNO") != null && mi.in.get("SUNO") != "") {
        inSUNO = mi.inData.get("SUNO").trim() 
        
        // Validate supplier if entered
        Optional<DBContainer> CIDMAS = findCIDMAS(inCONO, inSUNO)
        if (!CIDMAS.isPresent()) {
           mi.error("Supplier doesn't exist")   
           return             
        }

     } else {
        inSUNO = ""         
     }

     // Item Number
     if (mi.in.get("ITNO") != null && mi.in.get("ITNO") != "") {
        inITNO = mi.inData.get("ITNO").trim() 
        
        // Validate item if entered
        Optional<DBContainer> MITMAS = findMITMAS(inCONO, inITNO)
        if (!MITMAS.isPresent()) {
           mi.error("Item Number doesn't exist")   
           return             
        }

     } else {
        inITNO = ""         
     }
 
      // Date Posted
     if (mi.in.get("PODT") != null) {
        inPODT = mi.in.get("PODT") 
        
        //Validate date format
        boolean validPODT = utility.call("DateUtil", "isDateValid", String.valueOf(inPODT), "yyyyMMdd")  
        if (!validPODT) {
           mi.error("Posted Date is not valid")   
           return  
        } 

     } 

     // Due Date
     if (mi.in.get("DUDT") != null) {
        inDUDT = mi.in.get("DUDT") 
        
        //Validate date format
        boolean validDUDT = utility.call("DateUtil", "isDateValid", String.valueOf(inDUDT), "yyyyMMdd")  
        if (!validDUDT) {
           mi.error("Due Date is not valid")   
           return  
        } 

     } 

     // Batched Date
     if (mi.in.get("BADT") != null) {
        inBADT = mi.in.get("BADT") 
        
        //Validate date format
        boolean validBADT = utility.call("DateUtil", "isDateValid", String.valueOf(inBADT), "yyyyMMdd")  
        if (!validBADT) {
           mi.error("Batched Date is not valid")   
           return  
        } 

     }

     // Posted To
     if (mi.in.get("POTO") != null && mi.in.get("POTO") != "") {
        inPOTO = mi.inData.get("POTO").trim() 
     } else {
        inPOTO = ""         
     }

     // Net BF
     if (mi.in.get("NEBF") != null) {
        inNEBF = mi.in.get("NEBF") 
     } 
 
     // Amount
     if (mi.in.get("PIAM") != null) {
        inPIAM = mi.in.get("PIAM") 
     } 

     // Approve Date
     if (mi.in.get("APDT") != null) {
        inAPDT = mi.in.get("APDT") 
        
        //Validate date format
        boolean validAPDT = utility.call("DateUtil", "isDateValid", String.valueOf(inAPDT), "yyyyMMdd")  
        if (!validAPDT) {
           mi.error("Approve Date is not valid")   
           return  
        } 

     }

     // Approver
     if (mi.in.get("PIAU") != null && mi.in.get("PIAU") != "") {
        inPIAU = mi.inData.get("PIAU").trim() 
     } else {
        inPIAU = ""         
     }

     // Status
     if (mi.in.get("STAT") != null) {
        inSTAT = mi.in.get("STAT") 
     } 

     // Reference
     if (mi.in.get("TREF") != null && mi.in.get("TREF") != "") {
        inTREF = mi.inData.get("TREF").trim() 
     } else {
        inTREF = ""         
     }
     

     // Validate contract payment record
     Optional<DBContainer> EXTCPI = findEXTCPI(inCONO, inDIVI, inPINO, inCTNO, inRVID, inDLNO)
     if(!EXTCPI.isPresent()){
        mi.error("Contract Payment doesn't exist")   
        return             
     }     
    
     // Update record
     updEXTCPIRecord()
     
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
   // Check Supplier
   //******************************************************************** 
   private Optional<DBContainer> findCIDMAS(int CONO, String SUNO){  
     DBAction query = database.table("CIDMAS").index("00").build()   
     DBContainer CIDMAS = query.getContainer()
     CIDMAS.set("IDCONO", CONO)
     CIDMAS.set("IDSUNO", SUNO)
    
     if(query.read(CIDMAS))  { 
       return Optional.of(CIDMAS)
     } 
  
     return Optional.empty()
   }

  
   //******************************************************************** 
   // Check Item
   //******************************************************************** 
   private Optional<DBContainer> findMITMAS(int CONO, String ITNO){  
     DBAction query = database.table("MITMAS").index("00").build()   
     DBContainer MITMAS = query.getContainer()
     MITMAS.set("MMCONO", CONO)
     MITMAS.set("MMITNO", ITNO)
    
     if(query.read(MITMAS))  { 
       return Optional.of(MITMAS)
     } 
  
     return Optional.empty()
   }


  //******************************************************************** 
  // Update EXTCPI record
  //********************************************************************    
  void updEXTCPIRecord(){      
     DBAction action = database.table("EXTCPI").index("00").build()
     DBContainer EXTCPI = action.getContainer()     
     EXTCPI.set("EXCONO", inCONO)
     EXTCPI.set("EXDIVI", inDIVI)
     EXTCPI.set("EXPINO", inPINO)
     EXTCPI.set("EXCTNO", inCTNO)
     EXTCPI.set("EXRVID", inRVID)
     EXTCPI.set("EXDLNO", inDLNO)

     // Read with lock
     action.readLock(EXTCPI, updateCallBackEXTCPI)
     }
   
     Closure<?> updateCallBackEXTCPI = { LockedResult lockedResult -> 
       if (mi.in.get("SUNO") != null) {
          lockedResult.set("EXSUNO", mi.inData.get("SUNO").trim())
       }
  
       if (mi.in.get("ITNO") != null) {
          lockedResult.set("EXITNO", mi.inData.get("ITNO").trim())
       }
  
       if (mi.in.get("PODT") != null) {
          lockedResult.set("EXPODT", mi.in.get("PODT"))
       }
  
       if (mi.in.get("DUDT") != null) {
          lockedResult.set("EXDUDT", mi.in.get("DUDT"))
       }
  
       if (mi.in.get("BADT") != null) {
          lockedResult.set("EXBADT", mi.in.get("BADT"))
       }
  
       if (mi.in.get("POTO") != null) {
          lockedResult.set("EXPOTO", mi.inData.get("POTO").trim())
       }
  
       if (mi.in.get("NEBF") != null) {
          lockedResult.set("EXNEBF", mi.in.get("NEBF"))
       }
       
       if (mi.in.get("PIAM") != null) {
          lockedResult.set("EXPIAM", mi.in.get("PIAM"))
       }
       
       if (mi.in.get("APDT") != null) {
          lockedResult.set("EXAPDT", mi.in.get("APDT"))
       }
  
       if (mi.in.get("PIAU") != null) {
          lockedResult.set("EXPIAU", mi.inData.get("PIAU").trim())
       }
       if (mi.in.get("STAT") != null) {
          lockedResult.set("EXSTAT", mi.in.get("STAT"))
       }
  
       if (mi.in.get("TREF") != null) {
          lockedResult.set("EXTREF", mi.inData.get("TREF").trim())
       }

       int changeNo = lockedResult.get("EXCHNO")
       int newChangeNo = changeNo + 1 
       int changeddate = utility.call("DateUtil", "currentDateY8AsInt")
       lockedResult.set("EXLMDT", changeddate)       
       lockedResult.set("EXCHNO", newChangeNo) 
       lockedResult.set("EXCHID", program.getUser())
       lockedResult.update()
  }

} 

