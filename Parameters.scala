package com.optum.providerdashboard.common

/**
  * Created by smoham19 on 2/14/18.
  */
object Parameters {

  val  bicdbname = "uhc_bic"
  val  bicclaimpasssmrytbl = "providerdash_provider_claim_pass_summary"
  val roll12mnthstarttime = "add_months(trunc(current_date,'MM'),-12)"
  //need to delete the following
  //val roll12starttime = "date_sub(date(current_date), dayofmonth(current_date))"

  val roll12mnthendtime = "date_sub(date(current_date), dayofmonth(current_date))"
  //val previousyear = "year(current_date)-1"
  val pass1year = "year(current_date)-1"
  val pass2year = "year(current_date)-2"
  val curntyear = "year(current_date)"
  val pedbatchid = "pedbddev"
  val pedprvdsystbl = "ped_providersystems" 
  
  
/*val bicdbname  = "uhc_ped_dev"
val pvddbname  = "uhc_ped_demo"
val bicclmtable  = "provider_claim_pass_summary_03302018"
val pedbatchid  = "pedbddev"
val pedprvdsystbl  = "ped_providersystems"
val mongodbname  = "ped_dev"
val mongouname  = "fahmed"
val mongopasswd  = "O903jplo"
val mongohost  = "apsrp05644"
val mongoport  = "27017"*/

}
