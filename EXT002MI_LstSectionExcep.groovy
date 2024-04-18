// @author    Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
// @date      2023-04-10
// @version   1.0 
//
// Description 
// This API is to list section exception from EXTCSE
// Transaction LstSectionExcep
// AFMNI-7/Alias Replacement
// https://leanswift.atlassian.net/browse/AFMI-7

/**
 * IN
 * @param: CONO - Company Number
 * @param: DIVI - Division
 * @param: SEID - Exception ID
 * @param: DSID - Section ID
 * 
*/

/**
 * OUT
 * @return: CONO - Company Number
 * @return: DIVI - Division
 * @return: SEID - Exception ID
 * @return: DSID - Section ID
 * @return: ECOD - Section Code
 * 
*/

public class LstSectionExcep extends ExtendM3Transaction {
  private final MIAPI mi 
  private final DatabaseAPI database  
  private final ProgramAPI program
  
  Integer inCONO
  String inDIVI
  String inECOD
  int inDSID
  int numberOfFields

  
  // Constructor 
  public LstSectionExcep(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    
     // Exception Code
     if (mi.in.get("ECOD") != null && mi.in.get("ECOD") != "") {
        inECOD = mi.inData.get("ECOD").trim() 
     } else {
        inECOD = ""     
     }
     
     // Section ID
     if (mi.in.get("DSID") != null) {
        inDSID = mi.in.get("DSID") 
     } else {
        inDSID = 0     
     }


     // List section exception
     listSectionExcep()
  }

  //******************************************************************** 
  // List Contract Instructions from EXTCSE
  //******************************************************************** 
  void listSectionExcep(){ 
     ExpressionFactory expression = database.getExpressionFactory("EXTCSE")

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

     if (inECOD != "") {
       if (numberOfFields > 0) {
         expression = expression.and(expression.eq("EXECOD", inECOD))
         numberOfFields = 1
       } else {
         expression = expression.eq("EXECOD", inECOD)
         numberOfFields = 1
       }
     }

     if (inDSID != 0) {
       if (numberOfFields > 0) {
         expression = expression.and(expression.eq("EXDSID", String.valueOf(inDSID)))
         numberOfFields = 1
       } else {
         expression = expression.eq("EXDSID", String.valueOf(inDSID))
         numberOfFields = 1
       }
     }


     DBAction actionline = database.table("EXTCSE").index("00").matching(expression).selectAllFields().build()
	   DBContainer line = actionline.getContainer()   
     
     int pageSize = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords()    
     actionline.readAll(line, 0, pageSize, releasedLineProcessor)               
  }


    Closure<?> releasedLineProcessor = { DBContainer line -> 
      mi.outData.put("CONO", line.get("EXCONO").toString())
      mi.outData.put("DIVI", line.getString("EXDIVI"))
      mi.outData.put("SEID", line.get("EXSEID").toString()) 
      mi.outData.put("DSID", line.get("EXDSID").toString()) 
      mi.outData.put("ECOD", line.getString("EXECOD")) 
      mi.write() 
   } 
   
}