// @author    Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
// @date      2023-09-25
// @version   1.0 
//
// Description 
// This API will add contract payment information to EXTCPI
// Transaction AddContrPaymt
// AFMNI-7/Alias Replacement
// https://leanswift.atlassian.net/browse/AFMI-7

/**
 * IN
 * @param: CONO - Company Number
 * @param: DIVI - Division
 * @param: CTNO - Contract Number
 * @param: RVID - Revision ID 
 * @param: DLNO - Delivery Number
 * @param: SUNO - Supplier Number
 * @param: ITNO - Item Number
 * @param: PODT - Date Posted
 * @param: DUDT - Due Date
 * @param: BADT - Batched Date
 * @param: POTO - Posted to
 * @param: NEBF - Net BF
 * @param: PIAM - Amount
 * @param: APDT - Approve Date
 * @param: PIAU - Approver
 * @param: STAT - Status
 * @param: EXVL - Explicit Volume
 * @param: TREF - Reference
*/

/**
 * OUT
 * @param: CONO - Company Number
 * @param: DIVI - Division
 * @param: PINO - Payment Number
 * 
*/


public class AddContrPaymt extends ExtendM3Transaction {
  private final MIAPI mi 
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final LoggerAPI logger
  private final UtilityAPI utility
  
  // Definition 
  Integer inCONO
  String inDIVI
  String inRVID
  int inCTNO
  int inDLNO
  int outPINO
  String nextNumber
  String inSUNO
  String inITNO
  int inSTAT
  int inPODT
  int inDUDT
  int inBADT
  String inPOTO
  double inNEBF
  double inPIAM
  int inAPDT
  String inTREF
  String inPIAU
  int inEXVL

  
  // Constructor 
  public AddContrPaymt(MIAPI mi, DatabaseAPI database, MICallerAPI miCaller, ProgramAPI program, LoggerAPI logger, UtilityAPI utility) {
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

     // Contract Number 
     if (mi.in.get("CTNO") != null) {
        inCTNO = mi.in.get("CTNO") 
     } else {
        inCTNO = 0          
     }
      
     // Revision Number 
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
        inDUDT= mi.in.get("DUDT") 
        
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
     if (mi.in.get("POTO") != null) {
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
     
    // Explicit Volume
     if (mi.in.get("EXVL") != null) {
        inEXVL = mi.in.get("EXVL") 
     } 


     // Reference
     if (mi.in.get("TREF") != null && mi.in.get("TREF") != "") {
        inTREF = mi.inData.get("TREF").trim() 
     } else {
        inTREF = ""          
     }

     // Payment Number
     outPINO = 1
     
     //Get next number for payment number
     getNextNumber("", "L6", "1") 
     outPINO = nextNumber as Integer


     // Validate contract header
     Optional<DBContainer> EXTCTH = findEXTCTH(inCONO, inDIVI, inCTNO)
     if (!EXTCTH.isPresent()) {
        mi.error("Contract Number doesn't exist")   
        return  
     }

     // Validate revision
     Optional<DBContainer> EXTCTD = findEXTCTD(inCONO, inDIVI, inRVID)
     if (!EXTCTD.isPresent()) {
       mi.error("Contract Number/Revision ID doesn't exist")   
       return             
     }
     
     // Validate delivery header
     Optional<DBContainer> EXTDLH = findEXTDLH(inCONO, inDIVI, inDLNO)
     if (!EXTDLH.isPresent()) {
        mi.error("Delivery Number doesn't exist")   
        return  
     }


     // Write record 
     addEXTCPIRecord(inCONO, inDIVI, outPINO, inCTNO, inRVID, inDLNO, inSUNO, inITNO, inPODT, inDUDT, inBADT, inPOTO, inNEBF, inPIAM, inAPDT, inPIAU, inSTAT, inEXVL, inTREF)    
     
     mi.outData.put("CONO", String.valueOf(inCONO)) 
     mi.outData.put("DIVI", inDIVI) 
     mi.outData.put("PINO", String.valueOf(outPINO)) 
     mi.write()
  }
  
    
  //******************************************************************** 
  // Get EXTCTH record
  //******************************************************************** 
  private Optional<DBContainer> findEXTCTH(int CONO, String DIVI, int CTNO){  
     DBAction query = database.table("EXTCTH").index("00").build()
     DBContainer EXTCTH = query.getContainer()
     EXTCTH.set("EXCONO", CONO)
     EXTCTH.set("EXDIVI", DIVI)
     EXTCTH.set("EXCTNO", CTNO)
     if(query.read(EXTCTH))  { 
       return Optional.of(EXTCTH)
     } 
  
     return Optional.empty()
  }
  

  //******************************************************************** 
  // Get EXTCTD record
  //******************************************************************** 
  private Optional<DBContainer> findEXTCTD(int CONO, String DIVI, String RVID){  
     DBAction query = database.table("EXTCTD").index("00").build()
     DBContainer EXTCTD = query.getContainer()
     EXTCTD.set("EXCONO", CONO)
     EXTCTD.set("EXDIVI", DIVI)
     EXTCTD.set("EXRVID", RVID)
     if(query.read(EXTCTD))  { 
       return Optional.of(EXTCTD)
     } 
  
     return Optional.empty()
  }


   //***************************************************************************** 
   // Get next number in the number serie using CRS165MI.RtvNextNumber    
   // Input 
   // Division
   // Number Series Type
   // Number Seriew
   //***************************************************************************** 
   void getNextNumber(String division, String numberSeriesType, String numberSeries){   
        Map<String, String> params = [DIVI: division, NBTY: numberSeriesType, NBID: numberSeries] 
        Closure<?> callback = {
        Map<String, String> response ->
          if(response.NBNR != null){
            nextNumber = response.NBNR
          }
        }

        miCaller.call("CRS165MI","RtvNextNumber", params, callback)
   } 



  //******************************************************************** 
  // Get EXTDLH record
  //******************************************************************** 
  private Optional<DBContainer> findEXTDLH(int CONO, String DIVI, int DLNO){  
     DBAction query = database.table("EXTDLH").index("00").build()
     DBContainer EXTDLH = query.getContainer()
     EXTDLH.set("EXCONO", CONO)
     EXTDLH.set("EXDIVI", DIVI)
     EXTDLH.set("EXDLNO", DLNO)
     if(query.read(EXTDLH))  { 
       return Optional.of(EXTDLH)
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
  // Add EXTCPI record
  //********************************************************************     
  void addEXTCPIRecord(int CONO, String DIVI, int PINO, int CTNO, String RVID, int DLNO, String SUNO, String ITNO, int PODT, int DUDT, int BADT, String POTO, double NEBF, double PIAM, int APDT, String PIAU, int STAT, int EXVL, String TREF){     
       DBAction action = database.table("EXTCPI").index("00").build()
       DBContainer EXTCPI = action.createContainer()
       EXTCPI.set("EXCONO", CONO)
       EXTCPI.set("EXDIVI", DIVI)
       EXTCPI.set("EXPINO", PINO)
       EXTCPI.set("EXCTNO", CTNO)
       EXTCPI.set("EXRVID", RVID)
       EXTCPI.set("EXDLNO", DLNO)
       EXTCPI.set("EXSUNO", SUNO)
       EXTCPI.set("EXITNO", ITNO)
       EXTCPI.set("EXPODT", PODT)
       EXTCPI.set("EXDUDT", DUDT)
       EXTCPI.set("EXBADT", BADT)
       EXTCPI.set("EXPOTO", POTO)
       EXTCPI.set("EXNEBF", NEBF)
       EXTCPI.set("EXPIAM", PIAM)
       EXTCPI.set("EXAPDT", APDT)
       EXTCPI.set("EXPIAU", PIAU)
       EXTCPI.set("EXSTAT", STAT)
       EXTCPI.set("EXEXVL", EXVL)
       EXTCPI.set("EXTREF", TREF)
       EXTCPI.set("EXCHID", program.getUser())
       EXTCPI.set("EXCHNO", 1) 
       int regdate = utility.call("DateUtil", "currentDateY8AsInt")
       int regtime = utility.call("DateUtil", "currentTimeAsInt")                    
       EXTCPI.set("EXRGDT", regdate) 
       EXTCPI.set("EXLMDT", regdate) 
       EXTCPI.set("EXRGTM", regtime)
       action.insert(EXTCPI)         
 } 
 
 
} 

