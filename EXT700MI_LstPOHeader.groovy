/****************************************************************************************
 Extension Name: EXT700MI/LstPOHeader
 Type: ExtendM3Transaction
 Script Author: Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
 Date: 2023-09-08
 Description:
   This API transacation LstPOHeader is used to send PO data to ESKAR from M3
    
 Revision History:
 Name                    Date             Version          Description of Changes
 Jessica Bjorklund       2023-09-08       1.0              Creation
 Jessica Bjorklund       2025-09-18       2.0              Add logic for currency
******************************************************************************************/


public class LstPOHeader extends ExtendM3Transaction {
  private final MIAPI mi 
  private final DatabaseAPI database 
  private final ProgramAPI program
  private final LoggerAPI logger  
  private final MICallerAPI miCaller
  
  // Definition 
  public Integer company  
  public String inCONO
  public String inPUNO 
  public String inPNLI 
  public String inPNLS 
  public String division
  public String supplier
  public Integer orderDate  
  public double orderedAmount
  public String orderedAmountString
  public double sumOrderedAmount
  public double deliveredAmount
  public String deliveredAmountString
  public double sumDeliveredAmount
  public double invoicedAmount
  public String invoicedAmountString
  public double sumInvoicedAmount
  public double sumOrderedPerLine
  public double sumDeliveredPerLine
  public double sumInvoicedPerLine
  public double ordAmt
  public double delAmt
  public double invAmt
  public int pageSize = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords()    
    
  // Definition of output fields
  public String outCONO 
  public String outPUNO
  public String outNTAM  
  public String outDEAH
  public String outIVNA
  public String outDIVI
  public String outCUCD
  public String outDMCU
  public String outARAT
  public String outPUDT
  public String outCSCD
  
  // Constructor 
  public LstPOHeader(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger, MICallerAPI miCaller) {
     this.mi = mi
     this.database = database 
     this.program = program
     this.logger = logger 
     this.miCaller = miCaller
  } 
 

  //******************************************************************** 
  // Main 
  //********************************************************************  
  public void main() { 
    
    // Validate company
    company = mi.in.get("CONO")      
    if (company == null) {
      company = program.LDAZD.CONO as Integer
    } 
    inCONO = String.valueOf(company)
    inPUNO = mi.in.get("PUNO")
    
    clearOutput()

    //List PO to sum line info
    List<DBContainer> resultMPLINE = listMPLINE(company, inPUNO) 
    for (DBContainer recLineMPLINE : resultMPLINE){ 
        inPNLI = recLineMPLINE.get("IBPNLI")
        inPNLS = recLineMPLINE.get("IBPNLS")

        orderedAmountString = ""
        deliveredAmountString = ""
        invoicedAmountString = ""
        sumOrderedPerLine = 0d
        sumDeliveredPerLine = 0d
        sumInvoicedPerLine = 0d
        ordAmt = 0d
        delAmt = 0d
        invAmt = 0d
        
        lstPOInfoMI(inCONO, inPUNO, String.valueOf(inPNLI), String.valueOf(inPNLS))
        
        orderedAmount = Double.valueOf(orderedAmountString)
        deliveredAmount = Double.valueOf(deliveredAmountString)
        invoicedAmount = Double.valueOf(invoicedAmountString)

        sumOrderedAmount = sumOrderedAmount + orderedAmount
        sumDeliveredAmount = sumDeliveredAmount + deliveredAmount
        sumInvoicedAmount = sumInvoicedAmount + invoicedAmount
        
        outPUNO = inPUNO
        outNTAM = sumOrderedAmount
        outDEAH = sumDeliveredAmount
        outIVNA = sumInvoicedAmount
    }
    Optional<DBContainer> MPHEAD = findMPHEAD(company, inPUNO)
    if (MPHEAD.isPresent()) {
        DBContainer containerMPHEAD = MPHEAD.get()  
        
        logger.debug("resultMPHEAD")
        
        outDIVI = containerMPHEAD.getString("IADIVI")
        outCUCD = containerMPHEAD.getString("IACUCD")
        orderDate = containerMPHEAD.get("IAPUDT")
        supplier= containerMPHEAD.getString("IASUNO")
        outPUDT = String.valueOf(orderDate)
    }
    if(outDIVI!= null){   
        Optional<DBContainer> CMNDIV = findCMNDIV(company, outDIVI)
        if(CMNDIV.isPresent()){
           // Record found, continue to get information  
           DBContainer containerCMNDIV = CMNDIV.get() 
           outDMCU = String.valueOf(containerCMNDIV.get("CCDMCU"))   		  
        } 
    }    
    if(supplier!= null){
        Optional<DBContainer> CIDMAS = findCIDMAS(company, supplier)
        if(CIDMAS.isPresent()){
           // Record found, continue to get information  
           DBContainer containerCIDMAS = CIDMAS.get() 
           outCSCD = containerCIDMAS.getString("IDCSCD") 		  
        } 
    }
    if(outDIVI!= null){
       //Default if no rate found
       outARAT= "1.00"
       //Get the latest Currency Rate
       List<DBContainer> resultCCURRA = listCCURRA(company, outDIVI, outCUCD, 6, orderDate)
       for (DBContainer recLineCCURRA : resultCCURRA){ 
           // Record found, continue to get information  
           outARAT = '            '
           outARAT = String.valueOf(recLineCCURRA.get("CUARAT"))   		  
        }
    }        
    setOutput()
  } 


   //***************************************************************************** 
   // Get line info for calculation
   //***************************************************************************** 
   void lstPOInfoMI(String company, String purchaseOrder, String lineNumber, String lineSuffix){   
        Map<String, String> params = [CONO: company, PUNO: purchaseOrder, PNLI: lineNumber, PNLS: lineSuffix] 
        Closure<?> callback = {
          Map<String, String> response ->
          if(response.LNAM != null){
             orderedAmountString = response.LNAM 
             ordAmt = Double.valueOf(orderedAmountString)
             deliveredAmountString = response.DEAL
             delAmt = Double.valueOf(deliveredAmountString)
             invoicedAmountString = response.TIVA
             invAmt = Double.valueOf(invoicedAmountString)
          }
          sumOrderedPerLine = sumOrderedPerLine + ordAmt
          sumDeliveredPerLine = sumDeliveredPerLine + delAmt
          sumInvoicedPerLine = sumInvoicedPerLine + invAmt
        }
        
        miCaller.call("EXT700MI","LstPOData", params, callback)
   } 
  
  
  //******************************************************************** 
  // Read all lines for entered PO
  //********************************************************************  
  private List<DBContainer> listMPLINE(int CONO, String PUNO){
      List<DBContainer>recLineMPLINE = new ArrayList() 
      ExpressionFactory expression = database.getExpressionFactory("MPLINE")
      expression = expression.eq("IBPUNO", PUNO)
      
      DBAction query = database.table("MPLINE").index("00").matching(expression).selection("IBPNLI", "IBPNLS").build()
      DBContainer MPLINE = query.createContainer()
      MPLINE.set("IBCONO", CONO)
      
      query.readAll(MPLINE, 1, pageSize, { DBContainer recordMPLINE ->  
         recLineMPLINE.add(recordMPLINE.createCopy()) 
      })
  
      return recLineMPLINE
  }
  

  //******************************************************************** 
  // Get latest rate from CCURRA
  //******************************************************************** 
  private List<DBContainer> listCCURRA(Integer CONO, String DIVI, String CUCD, int CRTP, int CUTD){ 
    List<DBContainer>currencyLine = new ArrayList() 
    ExpressionFactory expression = database.getExpressionFactory("CCURRA")
    DBAction query = database.table("CCURRA").index("00").selection("CUARAT").reverse().build() 
    DBContainer CCURRA = query.getContainer() 
    CCURRA.set("CUCONO", CONO)
    CCURRA.set("CUDIVI", DIVI)
    CCURRA.set("CUCUCD", CUCD)
    CCURRA.set("CUCRTP", CRTP)
    CCURRA.set("CUCUTD", CUTD)

    query.readAll(CCURRA, 4, 1, { DBContainer record ->  
       currencyLine.add(record) 
    })
    
    return currencyLine
  } 
   
 
  //******************************************************************** 
  // Get information from MPHEAD
  //******************************************************************** 
  private Optional<DBContainer> findMPHEAD(Integer CONO, String PUNO){  
    DBAction query = database.table("MPHEAD").index("00").selection("IADIVI", "IACUCD", "IAPUDT", "IASUNO").build()    
    DBContainer MPHEAD = query.getContainer()
    MPHEAD.set("IACONO", CONO)
    MPHEAD.set("IAPUNO", PUNO)
    if(query.read(MPHEAD))  { 
      return Optional.of(MPHEAD)
    } 
  
    return Optional.empty()
  }

  //******************************************************************** 
  // Get Division information CMNDIV
  //******************************************************************** 
  private Optional<DBContainer> findCMNDIV(Integer CONO, String DIVI){  
    DBAction query = database.table("CMNDIV").index("00").selection("CCCONO","CCDMCU").build()
    DBContainer CMNDIV = query.getContainer()
    CMNDIV.set("CCCONO", CONO)
    CMNDIV.set("CCDIVI", DIVI)
    if(query.read(CMNDIV))  { 
      return Optional.of(CMNDIV)
    } 
  
    return Optional.empty()
  }    
  
  //******************************************************************** 
  // Get Supplier information CIDMAS
  //******************************************************************** 
  private Optional<DBContainer> findCIDMAS(Integer CONO, String SUNO){  
    DBAction query = database.table("CIDMAS").index("00").selection("IDCSCD").build()
    DBContainer CIDMAS = query.getContainer()
    CIDMAS.set("IDCONO", CONO)
    CIDMAS.set("IDSUNO", SUNO)
    if(query.read(CIDMAS))  { 
      return Optional.of(CIDMAS)
    } 
  
    return Optional.empty()
  }
  
 
  //******************************************************************** 
  // Clear Output data
  //******************************************************************** 
  void clearOutput() {
      outCONO = ""
      outPUNO = ""
      outNTAM = ""
      outDEAH = ""
      outIVNA = ""
      outDIVI = "" 
      outCUCD = ""
      outPUDT = ""
      outDMCU = ""
      outARAT = ""
      outCSCD = ""
  }
  
  
  //******************************************************************** 
  // Set Output data
  //******************************************************************** 
  void setOutput() {
      mi.outData.put("CONO", inCONO) 
      mi.outData.put("PUNO", inPUNO)
      mi.outData.put("NTAM", outNTAM)  
      mi.outData.put("DEAH", outDEAH)
      mi.outData.put("IVNA", outIVNA) 
      mi.outData.put("DIVI", outDIVI) 
      mi.outData.put("CUCD", outCUCD) 
      mi.outData.put("PUDT", outPUDT)
      mi.outData.put("DMCU", outDMCU) 
      mi.outData.put("ARAT", outARAT)
      mi.outData.put("CSCD", outCSCD)
  } 
    
}   
