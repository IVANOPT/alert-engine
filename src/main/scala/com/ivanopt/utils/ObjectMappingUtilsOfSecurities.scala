package com.ivanopt.utils


import com.ivanopt.bean.Securities

import scala.util.control.NonFatal

/**
  * Created by Ivan on 09/05/2018.
  */
object ObjectMappingUtilsOfSecurities {

  def unapply(values: Map[String, Object]) = try {
    Some(new Securities(values.get("WTH"), values.get("SBWTH"), values.get("KHH"), values.get("KHXM"), values.get("YYB"),
      values.get("KHQZ"), values.get("GSFL"), values.get("JYS"), values.get("GDH"), values.get("WTLB"),
      values.get("CXBZ"), values.get("CXWTH"), values.get("ZQDM"), values.get("ZQMC"), values.get("ZQLB"),
      values.get("DDLX"), values.get("DDJYXZ"), values.get("DDSXXZ"), values.get("DDYXRQ"), values.get("WTSL"),
      values.get("ZDPLSL"), values.get("WTJG"), values.get("ZSXJ"), values.get("WTRQ"), values.get("WTSJ"),
      values.get("BPGDH"), values.get("SBJB"), values.get("SBRQ"), values.get("SBSJ"), values.get("SBGY"),
      values.get("SBXW"), values.get("SBJLH"), values.get("JYSDDBH"), values.get("SBJG"), values.get("JGSM"), values.get("CDSL"),
      values.get("CJSL"), values.get("CJJE"), values.get("BRCJSL"), values.get("BRCJJE"), values.get("CJJG"),
      values.get("CJSJ"), values.get("BZ"), values.get("JSLX"), values.get("JSJG"), values.get("JSZH"),
      values.get("ZHGLJG"), values.get("LSH_ZQDJXM"), values.get("LSH_ZJDJXM"), values.get("DJZJ"), values.get("QSZJ"),
      values.get("FSYYB"), values.get("WTGY"), values.get("FHGY"), values.get("WTFS"), values.get("CZZD"),
      values.get("PLWTPCH"), values.get("YWSQH"), values.get("ISIN"), values.get("XWZBH"), values.get("TDBH"),
      values.get("BZS1"), values.get("SQCZRH"), values.get("CJBS"), values.get("STEP")
    ))
  } catch {
    case NonFatal(ex) => None
  }

}
