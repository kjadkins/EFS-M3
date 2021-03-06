// @author    Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
// @date      2021-09-27
// @version   1.0 
// 
// 2.0        220408  Additional fields added   Jessica Bjorklund
//
// Description 
// This API is to list open invoices from FSLEDG
// Transaction List
// 

import java.time.LocalDateTime;  
import java.time.format.DateTimeFormatter;

public class LstInvoices extends ExtendM3Transaction {
  private final MIAPI mi; 
  private final DatabaseAPI database; 
  private final ProgramAPI program;
  private final LoggerAPI logger; 
  private final MICallerAPI miCaller; 

  // Definition of output fields
  public String outCONO 
  public String outDIVI
  public String outPYNO 
  public String outCUNO 
  public String outCINO 
  public String outINYR
  public String outTRCD
  public String outYEA4
  public String outJRNO
  public String outJSNO
  public String outVSER
  public String outVONO
  public String outCUAM
  public String outRECO
  public String outREDE
  public String outIVDT
  public String outDUDT
  public String outPYCD
  public String outPYRS
  public String outIVTP
  public String outTDSC
  public String outARAT     
  public String outACAM     
  public String outCUCD     
  public String outCRTP     
  
  public Integer CONO
  public String inCONO 
  public int inRECO
  public int invoiceYear
  public double ARAT       
  public double CUAM       
  public double ACAM       
  public int YEA4          
  public int JRNO          
  public int JSNO          
  
  // Constructor 
  public LstInvoices(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger, MICallerAPI miCaller) {
     this.mi = mi;
     this.database = database;  
     this.program = program;
     this.logger = logger
     this.miCaller = miCaller;
  } 
    
  public void main() { 
     // Validate company
     CONO = mi.in.get("CONO")      
     if (CONO == null) {
        CONO = program.LDAZD.CONO as Integer
     } 
     Optional<DBContainer> CMNCMP = findCMNCMP(CONO)  
     if(!CMNCMP.isPresent()){                         
       mi.error("Company " + CONO + " is invalid")   
       return                                         
     }   

     inRECO = 9
     
     if (mi.in.get("INYR") != null) {
        invoiceYear = mi.in.get("INYR")
     } else {
        invoiceYear = 0
     }
     
     // Start the listing invoices in FSLEDG
     lstInvoices()

  }
 
  //******************************************************************** 
  // Get Company record
  //******************************************************************** 
  private Optional<DBContainer> findCMNCMP(Integer CONO){                             
      DBAction query = database.table("CMNCMP").index("00").selection("JICONO").build()   
      DBContainer CMNCMP = query.getContainer()                                           
      CMNCMP.set("JICONO", CONO)                                                         
      if(query.read(CMNCMP))  {                                                           
        return Optional.of(CMNCMP)                                                        
      }                                                                                  
      return Optional.empty()                                                            
  }                                                                                   



 //******************************************************************** 
  // Check if null or empty
  //********************************************************************  
  public  boolean isNullOrEmpty(String key) {
      if(key != null && !key.isEmpty())
          return false;
      return true;
  }

     
  //******************************************************************** 
  // Set Output data
  //******************************************************************** 
  void setOutPut() {     
    mi.outData.put("CONO", outCONO) 
    mi.outData.put("DIVI", outDIVI)
    mi.outData.put("PYNO", outPYNO)  
    mi.outData.put("CUNO", outCUNO)  
    mi.outData.put("CINO", outCINO) 
    mi.outData.put("INYR", outINYR)  
    mi.outData.put("TRCD", outTRCD)  
    mi.outData.put("YEA4", outYEA4)  
    mi.outData.put("JRNO", outJRNO)
    mi.outData.put("JSNO", outJSNO) 
    mi.outData.put("VSER", outVSER) 
    mi.outData.put("VONO", outVONO)  
    mi.outData.put("CUAM", outCUAM)  
    mi.outData.put("RECO", outRECO)  
    mi.outData.put("REDE", outREDE)  
    mi.outData.put("IVDT", outIVDT)  
    mi.outData.put("DUDT", outDUDT)
    mi.outData.put("PYCD", outPYCD) 
    mi.outData.put("PYRS", outPYRS)
    mi.outData.put("IVTP", outIVTP) 
    mi.outData.put("TDSC", outTDSC)  
    mi.outData.put("ARAT", outARAT)    
    mi.outData.put("ACAM", outACAM)    
    mi.outData.put("CUCD", outCUCD)    
    mi.outData.put("CRTP", outCRTP)    
  } 

   
   //******************************************************************** 
   // List all information
   //********************************************************************  
   void lstInvoices(){  
     
     // List all Invoice Delivery Lines
     ExpressionFactory expression = database.getExpressionFactory("FSLEDG")
   
     //Filter by invoice year if entered
     if (invoiceYear > 0) {
        expression = expression.ne("ESRECO", String.valueOf(inRECO)).and(expression.eq("ESTRCD", "10")).and(expression.eq("ESINYR", String.valueOf(invoiceYear)))
     } else {
        expression = expression.ne("ESRECO", String.valueOf(inRECO)).and(expression.eq("ESTRCD", "10"))
     }
     
     // List Invoice Delivery Lines   
     DBAction actionline = database.table("FSLEDG").index("19").matching(expression).selectAllFields().build()

     DBContainer line = actionline.getContainer()  
     
     // Read with one key  
     line.set("ESCONO", CONO)  
     
     int pageSize = mi.getMaxRecords() <= 0 ? 1000 : mi.getMaxRecords()           

     actionline.readAll(line, 1, pageSize, releasedLineProcessor)   
   
   } 

    
  //******************************************************************** 
  // List FSLEDG records - main loop
  //********************************************************************  
  Closure<?> releasedLineProcessor = { DBContainer line ->     
    
      // Output selectAllFields 
      outCONO = String.valueOf(line.get("ESCONO")) 
      outDIVI = String.valueOf(line.get("ESDIVI"))  
      outPYNO = String.valueOf(line.get("ESPYNO"))
      outCUNO = String.valueOf(line.get("ESCUNO"))      
      outCINO = String.valueOf(line.get("ESCINO"))
      outINYR = String.valueOf(line.get("ESINYR"))
      invoiceYear = line.get("ESINYR")
      outTRCD = String.valueOf(line.get("ESTRCD"))
      outYEA4 = String.valueOf(line.get("ESYEA4"))
      outJRNO = String.valueOf(line.get("ESJRNO"))
      outJSNO = String.valueOf(line.get("ESJSNO"))
      outVSER = String.valueOf(line.get("ESVSER"))
      outVONO = String.valueOf(line.get("ESVONO"))
      outRECO = line.get("ESRECO")
      outREDE = String.valueOf(line.get("ESREDE"))
      outIVDT = String.valueOf(line.get("ESIVDT"))
      outDUDT = String.valueOf(line.get("ESDUDT"))
      outPYCD = String.valueOf(line.get("ESPYCD"))
      outPYRS = String.valueOf(line.get("ESPYRS"))
      outIVTP = String.valueOf(line.get("ESIVTP"))
      outTDSC = String.valueOf(line.get("ESTDSC"))
      outARAT = String.valueOf(line.get("ESARAT"))   
      outCUCD = String.valueOf(line.get("ESCUCD"))   
      outCRTP = String.valueOf(line.get("ESCRTP"))   
      
      CUAM = line.get("ESCUAM")                      

      outCUAM = String.valueOf(CUAM)
      
      ARAT = line.get("ESARAT")                      
      ACAM = ARAT * CUAM                             
      outACAM = String.valueOf(ACAM)                 
      
      YEA4 = line.get("ESYEA4")                      
      JRNO = line.get("ESJRNO")                      
      JSNO = line.get("ESJSNO")                       
      
      // Send Output
      setOutPut()
      mi.write()    

  }
}