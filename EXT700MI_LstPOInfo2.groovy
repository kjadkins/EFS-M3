/****************************************************************************************
 Extension Name: EXT700MI/LstPOInfo2
 Type: ExtendM3Transaction
 Script Author: Jessica Bjorklund (jessica.bjorklund@columbusglobal.com)
 Date: 2023-09-08
 Description:
   This API transacation LstPOInfo2 is used to send PO data to ESKAR from M3
    
 Revision History:
 Name                    Date             Version          Description of Changes
 Jessica Bjorklund       2023-09-08       1.0              Creation
 Jessica Bjorklund       2025-09-18       2.0              Add logic for currency
 Jessica Bjorklund       2025-10-06       3.0              Get order type from MPHEAD to use in the U/M calc factor logic
******************************************************************************************/


import java.math.RoundingMode 

public class LstPOInfo2 extends ExtendM3Transaction {
  private final MIAPI mi 
  private final DatabaseAPI database 
  private final ProgramAPI program
  private final LoggerAPI logger  
  private final MICallerAPI miCaller
  
  // Definition 
  public Integer company  
  public String inCONO
  public String inPUNO 
  public int inPNLI 
  public int inPNLS 
  public String division
  public String supplier
  public String buyer 
  public double lineORQA 
  public double lineIVQA 
  public double lineRVQA   
  public double lineLNAM
  public double invLineIVNA 
  public String linePUUN 
  public double discount1 
  public double discount2  
  public double discount3 
  public double confirmedDiscount1 
  public double confirmedDiscount2 
  public double confirmedDiscount3 
  public double confirmedPrice  
  public double totalConfirmedDiscount
  public double totalDiscount
  public double purchasePrice
  public double linePrice
  public double lineAmount  
  public double invLineRCAC  
  public double invLineSERA  
  public double invLineIVQT  
  public double invlineICAC
  public double accRecRepQty
  public double accRecCostAmount 
  public double accRecInvAmount
  public double accRecExcRate  
  public double accRecQty  
  public double unitCOFA  
  public int unitDMCF 
  public double accInvQty  
  public double accInvAmount 
  public String cc_CountryCode
  public String id_CountryCode
  public String qtyConversionFactor  
  public double calcCOFA1   
  public double calcDMCF1   
  public double calcCOFA2   
  public double calcDMCF2 
  public double calcFACP
  public BigDecimal calcFACE
  public double calcFACO
  public double calcFACT
  public String linePPUN  
  public double invLineRPQA
  public String mplindLinePUOS
  public double mplindLineRPQA
  public int completeFlag  
  public double unitPrice
  public String status
  public Integer statusInt
  public Integer highestStatus
  public double repQty
  public String orderType
  public int pageSize = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords()        


  // Definition of output fields
  public String outLPUD
  public String outPNLI  
  public String outPNLS  
  public String outITNO  
  public String outLNAM
  public String outPITD  
  public String outORQT 
  public String outRVQT  
  public String outCONO 
  public String outDIVI
  public String outPUNO
  public String outSUNO   
  public String outPUDT 
  public String outEMAL
  public String outORIG
  public String outUNPR
  public String outDEAH
  public String outGRAM
  public String outDEAL 
  public String outSUDO 
  public String outRPQT
  public String outTRDT  
  public String outREPN 
  public String outIVQT
  public String outCOMP
  public String outGRIQ
  public String outGRIA
  public String outTIVA
  public String outIVNA
  public String outFACT  
  public String outVTCD
  public String outFACO
  public String outFACP
  public String outPUST
  public String outGRNR
  public String outARAT
  public String outDMCU
  public String outCSCD
  public String outCUCD
  public String outFACE
  
  
  // Constructor 
  public LstPOInfo2(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger, MICallerAPI miCaller) {
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
    inPNLI = mi.in.get("PNLI")
    inPNLS = mi.in.get("PNLS")
    

    clearOutput()
    lineORQA = 0d
    lineIVQA = 0d
    lineRVQA = 0d
    linePUUN = ""
    linePPUN = ""
    lineLNAM = 0d
    discount1 = 0d
    discount2 = 0d
    discount3 = 0d
    totalDiscount = 0d
    confirmedDiscount1 = 0d
    confirmedDiscount2 = 0d
    confirmedDiscount3 = 0d
    totalConfirmedDiscount = 0d
    confirmedPrice = 0d
    purchasePrice = 0d
    linePrice = 0d
    lineAmount = 0d
    unitPrice = 0d



    //***********************************************************************
    // Get Purchase order line
    //***********************************************************************
    Optional<DBContainer> MPLINE = findMPLINE(company, inPUNO, inPNLI, inPNLS)
    if (MPLINE.isPresent()) {
      // Record found, continue to get information  
      DBContainer containerMPLINE = MPLINE.get() 

      logger.debug("MPLINE found")
      
      // Output selectAllFields 
      outPUNO = inPUNO
      outLPUD = containerMPLINE.get("IBLPUD")
      outPNLI = containerMPLINE.get("IBPNLI")
      outPNLS = containerMPLINE.get("IBPNLS") 
      outITNO = containerMPLINE.get("IBITNO")
      outPITD = containerMPLINE.get("IBPITD")
      outVTCD = containerMPLINE.get("IBVTCD")
      outPUST  = containerMPLINE.get("IBPUST")
      if (outPUST >= "70" && outPUST <= "80") {
         outGRNR = "Y "
      } else {
         outGRNR = "N "
      }

      logger.debug("MPLINE outPUST ${outPUST}")
      logger.debug("MPLINE outGRNR ${outGRNR}")

      // Fields for calculation
      lineORQA = containerMPLINE.get("IBCFQA")
      if (lineORQA > 0) {
      } else {
         lineORQA = containerMPLINE.get("IBORQA")
      }
      lineIVQA = containerMPLINE.get("IBIVQA")
      lineRVQA = containerMPLINE.get("IBRVQA")
      linePUUN = containerMPLINE.get("IBPUUN")
      linePPUN = containerMPLINE.get("IBPPUN")
      lineLNAM = containerMPLINE.get("IBLNAM")
      
      discount1 = containerMPLINE.get("IBODI1")
      discount2 = containerMPLINE.get("IBODI2")
      discount3 = containerMPLINE.get("IBODI3")
      confirmedDiscount1 = containerMPLINE.get("IBCFD1")
      confirmedDiscount2 = containerMPLINE.get("IBCFD2")
      confirmedDiscount3 = containerMPLINE.get("IBCFD3")
      confirmedPrice = containerMPLINE.get("IBCPPR")
      purchasePrice = containerMPLINE.get("IBPUPR")

      //Calc the price
      linePrice = calcPrice(company, discount1, discount2, discount3, confirmedDiscount1, confirmedDiscount2, confirmedDiscount3, confirmedPrice, purchasePrice)
      
      lineAmount = linePrice * lineORQA


      Optional<DBContainer> MPHEAD = findMPHEAD(company, inPUNO)
      if (MPHEAD.isPresent()) {
        // Record found, continue to get information  
        DBContainer containerMPHEAD = MPHEAD.get()  
        
        logger.debug("resultMPHEAD")
        
        // output fields   
        outCONO = String.valueOf(containerMPHEAD.get("IACONO"))
        outDIVI = containerMPHEAD.getString("IADIVI")
		    outCUCD = containerMPHEAD.getString("IACUCD")
        outPUNO = containerMPHEAD.getString("IAPUNO")
        outSUNO = containerMPHEAD.getString("IASUNO")   
        outPUDT = String.valueOf(containerMPHEAD.get("IAPUDT"))
		    orderType = containerMPHEAD.getString("IAORTY")		
        // Fields from MPHEAD to use in the other read
        division = containerMPHEAD.getString("IADIVI")  
        supplier = containerMPHEAD.getString("IASUNO")  
        buyer = containerMPHEAD.getString("IABUYE")
       
        // Get Email address for Buyer
        Optional<DBContainer> CEMAIL = findCEMAIL(company, buyer)
        if(CEMAIL.isPresent()){
          // Record found, continue to get information  
          DBContainer containerCEMAIL = CEMAIL.get()    
          outEMAL = containerCEMAIL.getString("CBEMAL")
        } 
      
        // Get Supplier information 
        Optional<DBContainer> CIDMAS = findCIDMAS(company, supplier)
        if(CIDMAS.isPresent()){
          // Record found, continue to get information  
          DBContainer containerCIDMAS = CIDMAS.get() 
          id_CountryCode = containerCIDMAS.getString("IDCSCD")
		      outCSCD=id_CountryCode		  
        }  
         
        // Get division information
        Optional<DBContainer> CMNDIV = findCMNDIV(company, division)
        if(CMNDIV.isPresent()){
          // Record found, continue to get information  
          DBContainer containerCMNDIV = CMNDIV.get() 
          cc_CountryCode = containerCMNDIV.getString("CCCSCD")  
		      outDMCU = String.valueOf(containerCMNDIV.get("CCDMCU"))   		  
        } 
         
        // Compare division's country code and the Supplier's 
        if(cc_CountryCode != id_CountryCode){ 
          outORIG = String.valueOf("FOR")
        }else{ 
          outORIG = String.valueOf("DOM")
        } 
      }
	  
      //Get UoM factors
      getUoMFactors(company, outITNO, linePPUN, linePUUN)
      outFACP = String.valueOf(calcFACP)
      outFACO = String.valueOf(calcFACO)
      outFACT = String.valueOf(calcFACT)
      outFACE = calcFACE.setScale(6, RoundingMode.HALF_UP).toPlainString()  
	    outORQT = lineORQA
	    outRVQT = lineRVQA

      unitPrice = linePrice * calcFACT
      BigDecimal unitPriceRounded  = BigDecimal.valueOf(unitPrice) 
      unitPriceRounded = unitPriceRounded.setScale(4, RoundingMode.HALF_UP) 
      unitPrice = unitPriceRounded  

      outUNPR = String.valueOf(unitPrice) 

      //L Ordered Amount
      double lineOrderedAmount = lineORQA * (linePrice * calcFACT)
      BigDecimal lineOrderedAmountRounded  = BigDecimal.valueOf(lineOrderedAmount) 
      lineOrderedAmountRounded = lineOrderedAmountRounded.setScale(2, RoundingMode.HALF_UP) 
      lineOrderedAmount = lineOrderedAmountRounded  
      outLNAM = lineOrderedAmount

      outSUDO = ""  
      outRPQT = ""  
      outTRDT = ""  
      outREPN = ""  
      outGRIQ = ""  
      outGRAM = ""  
      outCOMP = ""  

      // Get information from receiving invoice lines - sum per PO line level
      outTIVA = ""  
      outIVQT = ""
      invLineIVNA = 0d 
      accInvQty = 0d 
      invLineRPQA = 0d
      accInvAmount = 0d 
      invLineIVQT = 0d
      List<DBContainer> resultFGINLIline = listFGINLIline(company, inPUNO, inPNLI, inPNLS) 
      for (DBContainer invLineLine : resultFGINLIline){   
          logger.debug("company ${company}")
          logger.debug("inPUNO ${inPUNO}")
          logger.debug("inPNLI ${inPNLI}")
          logger.debug("inPNLS ${inPNLS}")
          
	        invLineIVQT = invLineLine.get("F5IVQA")      
          invLineIVNA = invLineLine.get("F5IVNA")      
          
          logger.debug("resultFGINLIline invLineIVQT ${invLineIVQT}")
          logger.debug("resultFGINLIline invLineIVNA ${invLineIVNA}")
          
          accInvQty = accInvQty + invLineIVQT 
          accInvAmount = accInvAmount + invLineIVNA 
          
          logger.debug("resultFGINLIline accInvQty ${accInvQty}")
          logger.debug("resultFGINLIline accInvAmount ${accInvAmount}")
      } 
      
      outTIVA = String.valueOf(accInvAmount) 
      outIVQT = String.valueOf(accInvQty)
    } else {
      logger.debug("MPLINE not found")
    }

      logger.debug("MPLINE outPUNO ${outPUNO}")

      // Get information from receiving lines
      outSUDO = ""  
      outRPQT = ""  
      outTRDT = ""  
      outREPN = ""  
      outGRIQ = ""  
      outGRAM = ""  
      outCOMP = ""  

      double totalReceivedAmount = 0d        
      double totalReceivedAmountRounded = 0d   
      double totalReceivedQuantity = 0d        
      double totalReceivedQuantityRounded = 0d   

      
      List<DBContainer> resultFGRECLline = listFGRECLline(company, division, inPUNO, inPNLI, inPNLS) 
      for (DBContainer recLineLine : resultFGRECLline){
          int receivingNumber = recLineLine.get("F2REPN")  

          logger.debug("resultFGRECLline receivingNumber ${receivingNumber}")

          // Accumulate quantity   
          invLineRCAC = recLineLine.get("F2RCAC") 
          invLineSERA = recLineLine.get("F2SERA") 
          invLineIVQT = recLineLine.get("F2IVQA")  
          invLineRPQA = recLineLine.get("F2RPQA")   
          invlineICAC = recLineLine.get("F2ICAC")   
          
          //**********************************************************************************************************************
          // Accumulate quantity   
          double invLineSumRPQA = recLineLine.get("F2RPQA")    
          double invLineSumCAWE = recLineLine.get("F2CAWE")    

          logger.debug("invLineSumRPQA ${invLineSumRPQA}")
          logger.debug("invLineSumCAWE ${invLineSumCAWE}")

          if (invLineSumCAWE != 0) {
             invLineSumRPQA = invLineSumCAWE
          }
          
          logger.debug("invLineSumRPQA ${invLineSumRPQA}")
          
          checkMPLINDstatus(company, inPUNO, inPNLI, inPNLS, receivingNumber) 
          
          mplindLinePUOS = 0
          mplindLineRPQA = 0d
          logger.debug("highestStatus ${highestStatus}")
          if (highestStatus > 60) {
            List<DBContainer> resultMPLINDRep64 = listMPLIND(company, inPUNO, inPNLI, inPNLS, receivingNumber, "64") 
            for (DBContainer transLine : resultMPLINDRep64){
                mplindLineRPQA = transLine.get("ICRPQA")
            }
          } else {
               mplindLineRPQA = repQty
          }

          if (invLineSumRPQA > 0) {
            if (highestStatus == 60) {
              invLineSumRPQA = mplindLineRPQA
            } else if (highestStatus > 60) {
              invLineSumRPQA = invLineSumRPQA - mplindLineRPQA
            } else {
              invLineSumRPQA = 0d
            }
          } 
          
          //Sum Quanity
          logger.debug("invLineSumRPQA ${invLineSumRPQA}")

          totalReceivedQuantity = totalReceivedQuantity + invLineSumRPQA  
          
          logger.debug("totalReceivedQuantity ${totalReceivedQuantity}")
          
          BigDecimal totalReceivedQuantityFormat  = BigDecimal.valueOf(totalReceivedQuantity)   
          totalReceivedQuantityRounded = totalReceivedQuantityFormat.setScale(2, RoundingMode.HALF_UP)   
          outRVQT = totalReceivedQuantityRounded  

          logger.debug("invLineSumRPQA ${invLineSumRPQA}")  
          logger.debug("totalReceivedQuantityRounded ${totalReceivedQuantityRounded}")   
          
          //Sum Amount
          double invLineSumGRAM = totalReceivedQuantity * unitPrice     
          
          logger.debug("invLineSumGRAM ${invLineSumGRAM}")

          totalReceivedAmount = invLineSumGRAM  
          
          logger.debug("totalReceivedAmount ${totalReceivedAmount}")
          
          BigDecimal totalReceivedAmountFormat  = BigDecimal.valueOf(totalReceivedAmount)  
          totalReceivedAmountRounded = totalReceivedAmountFormat.setScale(2, RoundingMode.HALF_UP)  
          outDEAL = totalReceivedAmountRounded  

          logger.debug("invLineSumGRAM ${invLineSumGRAM}") 
          logger.debug("totalReceivedAmountRounded ${totalReceivedAmountRounded}")   
		  
          //***********************************************************************************************************************

          accRecCostAmount = accRecCostAmount + invLineRCAC   
          accRecInvAmount = accRecInvAmount + invlineICAC
          accRecExcRate = invLineSERA    
		      outARAT=accRecExcRate
          accRecRepQty = accRecRepQty + invLineRPQA
          accRecQty = accRecQty + (invLineIVQT * lineAmount)  
          
          double recLineGRAmount = accRecRepQty * (linePrice * calcFACT)
          BigDecimal recLineGRAmountRounded  = BigDecimal.valueOf(recLineGRAmount) 
          recLineGRAmountRounded = recLineGRAmountRounded.setScale(2, RoundingMode.HALF_UP) 
          recLineGRAmount = recLineGRAmountRounded  

          //Get receiving info
          mplindLinePUOS = 0
          mplindLineRPQA = 0d
          List<DBContainer> resultMPLIND = listMPLIND(company, inPUNO, inPNLI, inPNLS, receivingNumber, "0") 
          for (DBContainer transLine : resultMPLIND){
            
              logger.debug("resultMPLIND")
            
              mplindLinePUOS = transLine.get("ICPUOS")   
              mplindLineRPQA = transLine.get("ICRPQA")
              completeFlag = transLine.get("ICOEND")

              if(completeFlag == 1){ 
                 outCOMP = "Yes" 
              }else{ 
                 outCOMP = "No" 
              } 
          }

          repQty = 0d
          checkMPLINDstatus(company, inPUNO, inPNLI, inPNLS, receivingNumber) 
          
          mplindLinePUOS = 0
          mplindLineRPQA = 0d
          if (highestStatus > 60) {
            List<DBContainer> resultMPLINDRep64 = listMPLIND(company, inPUNO, inPNLI, inPNLS, receivingNumber, "64") 
            for (DBContainer transLine : resultMPLINDRep64){
                mplindLineRPQA = transLine.get("ICRPQA")
            }
          } else {
               mplindLineRPQA = repQty
          }

          // Output   
          outSUDO = String.valueOf(recLineLine.get("F2SUDO")) 
          double CAWE =  recLineLine.get("F2CAWE")
          double RPQT =  recLineLine.get("F2RPQA")
          
          logger.debug("CAWE ${CAWE}")
          logger.debug("RPQT 1 ${RPQT}")
          
          if (CAWE != 0) {
             RPQT = CAWE
          }
          
          logger.debug("RPQT 2 ${RPQT}")
          
          if (RPQT > 0) {
            if (highestStatus == 60) {
              RPQT = mplindLineRPQA
            } else if (highestStatus > 60) {
              RPQT = RPQT - mplindLineRPQA
            }
          } 
          
          logger.debug("RPQT 3 ${RPQT}")
          
          outRPQT = String.valueOf(RPQT) 

          double receivedAmount = 0d
          
          logger.debug("RPQT last ${RPQT}")
          logger.debug("unitPrice last ${unitPrice}")
          
          receivedAmount = RPQT * unitPrice
          logger.debug("receivedAmount last ${receivedAmount}")
          BigDecimal receivedAmountRounded  = BigDecimal.valueOf(receivedAmount) 
          receivedAmountRounded = receivedAmountRounded.setScale(2, RoundingMode.HALF_UP) 
          receivedAmount = receivedAmountRounded  

          outGRAM = String.valueOf(receivedAmount)
          outTRDT = String.valueOf(recLineLine.get("F2TRDT"))  
          outREPN = String.valueOf(recLineLine.get("F2REPN")) 
          outGRIQ = String.valueOf(recLineLine.get("F2IVQA")) 
          double GRIQ =  recLineLine.get("F2IVQA")
          BigDecimal grInvoicedAmountFormat  = BigDecimal.valueOf(GRIQ * unitPrice)  
          double grInvoicedAmountRounded = grInvoicedAmountFormat.setScale(2, RoundingMode.HALF_UP)  
          outGRIA = String.valueOf(grInvoicedAmountRounded)
          
          setOutput()
          mi.write()   

      } 

  } 
  
  //*******************************************************************************************
  // Calc the price
  //********************************************************************************************
  private double calcPrice(int CONO, double ODI1, double ODI2, double ODI3, double CFD1, double CFD2, double CFD3, double CPPR, double PUPR) { 
      totalDiscount = ODI1 + ODI2 + ODI3
      totalConfirmedDiscount = CFD1 + CFD2 + CFD3

      if (CPPR > 0) {
         linePrice = (CPPR * (100 - totalConfirmedDiscount))/100
      } else {
         linePrice = (PUPR * (100 - totalDiscount))/100
      }
      
      return linePrice
  }
  
  
  //**************************************************************************************** 
  // Get and set Unit of Measure Factors 
  //*****************************************************************************************
  void getUoMFactors(int CONO, String ITNO, String PPUN, String PUUN) {
	  calcFACE = 0d
      calcFACP = 0d
      calcFACO = 0d
      calcFACT = 0d
      
      Optional<DBContainer> MITAUN0 = findMITAUN(company, outITNO, 1, linePPUN)
      if(MITAUN0.isPresent()){
        qtyConversionFactor='Y'
        
      } else{
        qtyConversionFactor='N'	
		}

      // Calculate with Unit of measure factor   
      // Get COFA and DMCF from PPUN Price UoM   
      Optional<DBContainer> MITAUN1 = findMITAUN(company, outITNO, 2, linePPUN)
      if(MITAUN1.isPresent()){
        // Record found, continue to get information  
        DBContainer containerMITAUN1 = MITAUN1.get() 
        calcCOFA1 = containerMITAUN1.get("MUCOFA")
        calcDMCF1 = containerMITAUN1.get("MUDMCF") 
      } else { 
        calcCOFA1 = 1
        calcDMCF1 = 1
      }
      if (calcDMCF1 == 1) {
        calcFACP = 1/calcCOFA1
		   if((qtyConversionFactor == 'N')||(orderType == 'PC2' && linePUUN != linePPUN)){
		    calcFACE = 1/calcCOFA1
		  }else {
		    calcFACE=1
		  }
        } else {
          calcFACP = calcCOFA1
		   if((qtyConversionFactor == 'N')||(orderType == 'PC2' && linePUUN != linePPUN)){
		    calcFACE = calcCOFA1
		  }else {
		    calcFACE=1
		  }
    }

      // Get COFA and DMCF from PUUN Qty UoM           
      Optional<DBContainer> MITAUN2 = findMITAUN(company, outITNO, 2, linePUUN)
      if(MITAUN2.isPresent()){
        // Record found, continue to get information  
        DBContainer containerMITAUN2 = MITAUN2.get() 
          calcCOFA2 = containerMITAUN2.get("MUCOFA")
          calcDMCF2 = containerMITAUN2.get("MUDMCF") 
        } else { 
          calcCOFA2 = 1
          calcDMCF2 = 1

        }
        if (calcDMCF2 == 1) {
          calcFACO = calcCOFA2
        } else {
          calcFACO = 1/calcCOFA2
        }
        calcFACT = calcFACP * calcFACO
  }
    
                
  //******************************************************************** 
  // Get Company from LDA
  //******************************************************************** 
  private Integer getCONO() {
    int company = mi.in.get("CONO") as Integer
    if(company == null){
      company = program.LDAZD.CONO as Integer
    } 
    return company
    
  } 

  
  //******************************************************************** 
  // Get Division information CMNDIV
  //******************************************************************** 
  private Optional<DBContainer> findCMNDIV(Integer CONO, String DIVI){  
    DBAction query = database.table("CMNDIV").index("00").selection("CCCONO", "CCCSCD", "CCDIVI","CCDMCU").build()
    DBContainer CMNDIV = query.getContainer()
    CMNDIV.set("CCCONO", CONO)
    CMNDIV.set("CCDIVI", DIVI)
    if(query.read(CMNDIV))  { 
      return Optional.of(CMNDIV)
    } 
  
    return Optional.empty()
  }
   
   
  //******************************************************************** 
  // Get Division information MPLINE
  //******************************************************************** 
  private Optional<DBContainer> findMPLINE(Integer CONO, String PUNO, Integer PNLI, Integer PNLS){  
    DBAction query = database.table("MPLINE").index("00").selection("IBPUPR", "IBCONO", "IBPUNO", "IBPNLI", "IBPNLS", "IBITNO", "IBLPUD", "IBLNAM", "IBPITD", "IBVTCD", "IBORQA", "IBIVQA", "IBRVQA", "IBPUUN", "IBPPUN", "IBODI1", "IBODI2", "IBODI3", "IBCFD1", "IBCFD2", "IBCFD3", "IBCPPR", "IBPUST").build()    
    DBContainer MPLINE = query.getContainer()
    MPLINE.set("IBCONO", CONO)
    MPLINE.set("IBPUNO", PUNO)
    MPLINE.set("IBPNLI", PNLI)
    MPLINE.set("IBPNLS", PNLS)
    if(query.read(MPLINE))  { 
      return Optional.of(MPLINE)
    } 
  
    return Optional.empty()
  } 


  //******************************************************************** 
  // Get information from MPHEAD
  //******************************************************************** 
  private Optional<DBContainer> findMPHEAD(Integer CONO, String PUNO){  
    DBAction query = database.table("MPHEAD").index("00").selection("IACONO", "IAPUNO", "IADIVI", "IASUNO", "IANTAM", "IAPUDT", "IABUYE", "IACOAM","IACUCD","IAORTY").build()    
    DBContainer MPHEAD = query.getContainer()
    MPHEAD.set("IACONO", CONO)
    MPHEAD.set("IAPUNO", PUNO)
    if(query.read(MPHEAD))  { 
      return Optional.of(MPHEAD)
    } 
  
    return Optional.empty()
  } 
  
  
  //******************************************************************** 
  // Accumulate value from FGINLI - PO line level
  //******************************************************************** 
  private List<DBContainer> listFGINLIline(Integer CONO, String PUNO, Integer PNLI, Integer PNLS){ 
    List<DBContainer>invLine = new ArrayList() 
    ExpressionFactory expression = database.getExpressionFactory("FGINLI")
    DBAction query = database.table("FGINLI").index("20").selection("F5IVQA", "F5IVNA").reverse().build()   
    DBContainer FGINLIline = query.getContainer() 
    FGINLIline.set("F5CONO", CONO)   
    FGINLIline.set("F5PUNO", PUNO) 
    FGINLIline.set("F5PNLI", PNLI) 
    FGINLIline.set("F5PNLS", PNLS) 

    query.readAll(FGINLIline, 4, pageSize, { DBContainer record ->                                           
      logger.debug("FGINLI record ${record}")
      invLine.add(record.createCopy()) 
    })

    logger.debug("invLine ${invLine}")
    
    return invLine
  } 
  
  
  //******************************************************************** 
  // Get record from MPLIND
  //******************************************************************** 
  private List<DBContainer> listMPLIND(int CONO, String PUNO, int PNLI, int PNLS, int REPN, String PUOS){ 
    List<DBContainer>transLine = new ArrayList() 
    ExpressionFactory expression = database.getExpressionFactory("MPLIND")
    DBAction query = database.table("MPLIND").index("60").selection("ICOEND", "ICRPQA", "ICPUOS").build() 
    DBContainer MPLIND = query.getContainer() 
    MPLIND.set("ICCONO", CONO)   
    MPLIND.set("ICPUNO", PUNO) 
    MPLIND.set("ICPNLI", PNLI) 
    MPLIND.set("ICPNLS", PNLS) 
    MPLIND.set("ICREPN", REPN) 
    MPLIND.set("ICPUOS", PUOS)

    if (PUOS.equals("0")) {
       query.readAll(MPLIND, 5, pageSize, { DBContainer record ->  
       transLine.add(record) 
      })
    } else {
       query.readAll(MPLIND, 6, pageSize, { DBContainer record ->  
       transLine.add(record) 
      })
    }
    
    return transLine
  } 
   

   //******************************************************************** 
   // Check highest status in MPLIND
   //********************************************************************  
   void checkMPLINDstatus(int CONO, String PUNO, int PNLI, int PNLS, int REPN){ 
     
     highestStatus = 0

     //Read all to check highest status
     DBAction queryMPLIND = database.table("MPLIND").index("60").selection("ICOEND", "ICRPQA", "ICPUOS").build() 
     DBContainer containerMPLIND = queryMPLIND.getContainer()
     containerMPLIND.set("ICCONO", CONO)
     containerMPLIND.set("ICPUNO", PUNO)
     containerMPLIND.set("ICPNLI", PNLI)
     containerMPLIND.set("ICPNLS", PNLS)
     containerMPLIND.set("ICREPN", REPN)

     queryMPLIND.readAll(containerMPLIND, 5, pageSize, releasedLineProcessorMPLIND)
   } 

    
  //******************************************************************** 
  // Check status - main loop
  //********************************************************************  
  Closure<?> releasedLineProcessorMPLIND = { DBContainer containerMPLIND -> 
      status = containerMPLIND.get("ICPUOS") 
      statusInt = Integer.valueOf(status)
      repQty = containerMPLIND.get("ICRPQA") 

      if (statusInt > highestStatus) {
         highestStatus = statusInt
      }
  }

   
  //******************************************************************** 
  // Accumulate value from FGRECL - PO Line level
  //********************************************************************  
  private List<DBContainer> listFGRECLline(int CONO, String DIVI, String PUNO, int PNLI, int PNLS){
    List<DBContainer>recLineLine = new ArrayList() 
    ExpressionFactory expression = database.getExpressionFactory("FGRECL")
    expression = expression.ne("F2RPQA", String.valueOf(0)) 
    DBAction query = database.table("FGRECL").index("00").matching(expression).selection("F2REPN", "F2RCAC", "F2SERA", "F2IVQA", "F2RPQA", "F2ICAC", "F2SUDO", "F2CAWE", "F2TRDT").build()
    DBContainer FGRECLline = query.createContainer()
    FGRECLline.set("F2CONO", CONO)
    FGRECLline.set("F2DIVI", DIVI)
    FGRECLline.set("F2PUNO", PUNO)
    FGRECLline.set("F2PNLI", PNLI)
    FGRECLline.set("F2PNLS", PNLS)   

    query.readAll(FGRECLline, 5, pageSize, { DBContainer record ->  
      recLineLine.add(record.createCopy()) 
    })

    return recLineLine
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
  // Get Email address CEMAIL
  //******************************************************************** 
  private Optional<DBContainer> findCEMAIL(Integer CONO, String BUYE){   
    DBAction query = database.table("CEMAIL").index("00").selection("CBEMAL").build()
    DBContainer CEMAIL = query.getContainer()
    CEMAIL.set("CBCONO", CONO)
    CEMAIL.set("CBEMTP", "04")
    CEMAIL.set("CBEMKY", BUYE)
    if(query.read(CEMAIL))  { 
      return Optional.of(CEMAIL)
    } 
    
    return Optional.empty()
  }
  
  
  //******************************************************************** 
  // Get Alternativ unit MITAUN
  //******************************************************************** 
   private Optional<DBContainer> findMITAUN(Integer CONO, String ITNO, Integer AUTP, String ALUN){  
    DBAction query = database.table("MITAUN").index("00").selection("MUCOFA", "MUDMCF", "MUAUTP", "MUALUN").build()
    DBContainer MITAUN = query.getContainer()
    MITAUN.set("MUCONO", CONO)
    MITAUN.set("MUITNO", ITNO)
    MITAUN.set("MUAUTP", AUTP)
    MITAUN.set("MUALUN", ALUN)
    if(query.read(MITAUN))  { 
      return Optional.of(MITAUN)
    } 
    
    return Optional.empty()
  }   
  

  //******************************************************************** 
  // Clear Output data
  //******************************************************************** 
  void clearOutput() {
    outCONO = ""
    outDIVI = ""
    outPUNO = ""
    outSUNO = ""
    outLPUD = ""
    outPNLI = ""  
    outPNLS = "" 
    outITNO = "" 
    outLNAM = ""
    outPITD = "" 
    outORQT = ""
    outIVQT = ""
    outRVQT = ""    
    outPUDT = ""
    outEMAL = "" 
    outORIG = ""
    outUNPR = ""
    outDEAH = ""
    outGRAM = ""
    outDEAL = ""
    outSUDO = ""
    outRPQT = ""
    outTRDT = ""
    outREPN = ""
    outGRIQ = ""
    outCOMP = ""
    outGRIA = ""
    outTIVA = ""
    outIVNA = ""
    outFACT = ""
    outVTCD = ""   
    outFACO = ""
    outFACP = ""
    outPUST = ""
    outGRNR = ""
    outARAT = ""
    outDMCU = ""
    outFACE = ""
	  
  }
  
  
  //******************************************************************** 
  // Set Output data
  //******************************************************************** 
  void setOutput() {
    mi.outData.put("CONO", outCONO)  
    mi.outData.put("DIVI", outDIVI)
    mi.outData.put("PUNO", outPUNO)
    mi.outData.put("SUNO", outSUNO)
    mi.outData.put("LPUD", outLPUD)
    mi.outData.put("PNLI", outPNLI)  
    mi.outData.put("PNLS", outPNLS)  
    mi.outData.put("ITNO", outITNO)  
    mi.outData.put("LNAM", outLNAM)
    mi.outData.put("PITD", outPITD)  
    mi.outData.put("ORQT", outORQT)
    mi.outData.put("IVQT", outIVQT)
    mi.outData.put("RVQT", outRVQT)    
    mi.outData.put("PUDT", outPUDT) 
    mi.outData.put("EMAL", outEMAL) 
    mi.outData.put("ORIG", outORIG)
    mi.outData.put("UNPR", outUNPR)
    mi.outData.put("GRAM", outGRAM)
    mi.outData.put("DEAL", outDEAL) 
    mi.outData.put("SUDO", outSUDO) 
    mi.outData.put("RPQT", outRPQT) 
    mi.outData.put("TRDT", outTRDT)  
    mi.outData.put("REPN", outREPN) 
    mi.outData.put("GRIQ", outGRIQ)
    mi.outData.put("COMP", outCOMP)
    mi.outData.put("GRIA", outGRIA)
    mi.outData.put("TIVA", outTIVA)
    mi.outData.put("FACT", outFACT) 
    mi.outData.put("VTCD", outVTCD)    
    mi.outData.put("FACO", outFACO) 
    mi.outData.put("FACP", outFACP) 
    mi.outData.put("PUST", outPUST) 
    mi.outData.put("GRNR", outGRNR) 
  	mi.outData.put("ARAT", outARAT) 
  	mi.outData.put("DMCU", outDMCU) 
  	mi.outData.put("CSCD", outCSCD) 
  	mi.outData.put("CUCD", outCUCD) 
  	mi.outData.put("FACE", outFACE) 
  } 
    
}   

