// @author    Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
// @date      2023-09-25
// @version   1.0 
//
// Description 
// This API is to list contract payments from EXTCPI
// Transaction LstContrPaymt
// AFMNI-7/Alias Replacement
// https://leanswift.atlassian.net/browse/AFMI-7

/**
 * IN
 * @param: CONO - Company Number
 * @param: DIVI - Division
 * @param: DLNO - Delivery Number
 * @param: STLR - Log Rule
 * 
*/

/**
 * OUT
 * @return: CONO - Company Number
 * @return: DIVI - Division
 * @return: DLNO - Delivery Number
 * @return: STNO - Scale Ticket Number
 * @return: STDT - Scale Date
 * @return: STLR - Log Rule
 * @return: STLN - Scale Location Number
 * @return: STSN - Scaler Number
 * @return: STID - Scale Ticket ID
 * @return: STLP - Log Percentage

 * 
*/

public class LstContrPaymt extends ExtendM3Transaction {
  private final MIAPI mi 
  private final DatabaseAPI database  
  private final ProgramAPI program
  
  Integer inCONO
  String inDIVI
  int inCTNO
  int inDLNO
  String inSUNO
  String inPIAU
  int inSTAT
  String inTREF
  int numberOfFields
  
  // Constructor 
  public LstContrPaymt(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
     this.mi = mi
     this.database = database 
     this.program = program
  } 
    
  public void main() { 
     // Set Company Number
     if (mi.in.get("CONO") != null) {
        inCONO = mi.in.get("CONO") 
     } else {
        inCONO = 0      
     }

     // Set Division
     if (mi.in.get("DIVI") != null) {
        inDIVI = mi.inData.get("DIVI").trim() 
     } else {
        inDIVI = ""     
     }

     // Contract Number
     if (mi.in.get("CTNO") != null) {
        inCTNO = mi.in.get("CTNO") 
     } else {
        inCTNO = 0      
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
     } else {
        inSUNO = ""      
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
     } else {
        inSTAT = 0      
     }

     // Reference
     if (mi.in.get("TREF") != null && mi.in.get("TREF") != "") {
        inTREF = mi.inData.get("TREF").trim() 
     } else {
        inTREF = ""      
     }


     // List contract payments from EXTCPI
     listContractPayments()
  }
 
  //******************************************************************** 
  // List Contract Payments 
  //******************************************************************** 
  void listContractPayments(){ 
     ExpressionFactory expression = database.getExpressionFactory("EXTCPI")

     numberOfFields = 0

     if (inCONO != 0) {
       numberOfFields = 1
       expression = expression.eq("EXCONO", String.valueOf(inCONO))
     }

     if (inDIVI != "") {
       if (numberOfFields > 0) {
         expression = expression.and(expression.eq("EXDIVI", inDIVI))
         numberOfFields = 1
       } else {
         expression = expression.eq("EXDIVI", inDIVI)
         numberOfFields = 1
       }
     }

     if (inCTNO != 0) {
       if (numberOfFields > 0) {
         expression = expression.and(expression.eq("EXCTNO", String.valueOf(inCTNO)))
         numberOfFields = 1
       } else {
         expression = expression.eq("EXCTNO", String.valueOf(inCTNO))
         numberOfFields = 1
       }
     }

     if (inDLNO != 0) {
       if (numberOfFields > 0) {
         expression = expression.and(expression.eq("EXDLNO", String.valueOf(inDLNO)))
         numberOfFields = 1
       } else {
         expression = expression.eq("EXDLNO", String.valueOf(inDLNO))
         numberOfFields = 1
       }
     }

     if (inSUNO != "") {
       if (numberOfFields > 0) {
         expression = expression.and(expression.eq("EXSUNO", inSUNO))
         numberOfFields = 1
       } else {
         expression = expression.eq("EXSUNO", inSUNO)
         numberOfFields = 1
       }
     }

     if (inPIAU != "") {
       if (numberOfFields > 0) {
         expression = expression.and(expression.eq("EXPIAU", inPIAU))
         numberOfFields = 1
       } else {
         expression = expression.eq("EXPIAU", inPIAU)
         numberOfFields = 1
       }
     }
     
     if (inSTAT != 0) {
       if (numberOfFields > 0) {
         expression = expression.and(expression.eq("EXSTAT", String.valueOf(inSTAT)))
         numberOfFields = 1
       } else {
         expression = expression.eq("EXSTAT", String.valueOf(inSTAT))
         numberOfFields = 1
       }
     }

     if (inTREF != "") {
       if (numberOfFields > 0) {
         expression = expression.and(expression.eq("EXTREF", inTREF))
         numberOfFields = 1
       } else {
         expression = expression.eq("EXTREF", inTREF)
         numberOfFields = 1
       }
     }

     DBAction actionline = database.table("EXTCPI").index("00").matching(expression).selectAllFields().build()
	   DBContainer line = actionline.getContainer()   
     
     int pageSize = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords()       
     actionline.readAll(line, 0, pageSize, releasedLineProcessor)               
   } 

    Closure<?> releasedLineProcessor = { DBContainer line -> 
      mi.outData.put("CONO", line.get("EXCONO").toString())
      mi.outData.put("DIVI", line.getString("EXDIVI"))
      mi.outData.put("PINO", line.get("EXPINO").toString())
      mi.outData.put("CTNO", line.get("EXCTNO").toString())
      mi.outData.put("RVID", line.getString("EXRVID"))
      mi.outData.put("DLNO", line.get("EXDLNO").toString())
      mi.outData.put("SUNO", line.getString("EXSUNO"))
      mi.outData.put("ITNO", line.getString("EXITNO"))
      mi.outData.put("PODT", line.get("EXPODT").toString())
      mi.outData.put("DUDT", line.get("EXDUDT").toString())
      mi.outData.put("BADT", line.get("EXBADT").toString())
      mi.outData.put("POTO", line.getString("EXPOTO"))
      mi.outData.put("NEBF", line.getDouble("EXNEBF").toString())
      mi.outData.put("PIAM", line.getDouble("EXPIAM").toString())
      mi.outData.put("APDT", line.get("EXAPDT").toString())
      mi.outData.put("PIAU", line.getString("EXPIAU"))
      mi.outData.put("STAT", line.get("EXSTAT").toString())
      mi.outData.put("EXVL", line.get("EXEXVL").toString())
      mi.outData.put("TREF", line.getString("EXTREF"))
      mi.write() 
   } 
   
}