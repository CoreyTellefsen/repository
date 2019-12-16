
*********************************************************************************

Libraries

*********************************************************************************;

libname Takeda2 sas7bdat 'D:\Takeda';
libname Takeda sas7bdat 'C:\Users\Corey_Tellefsen\Desktop\Projects\Company\Takeda';


*********************************************************************************

Create shortened tables with only necessary variables for smaller table sizes and faster processing


*********************************************************************************;

proc sql;
	create table product_hierarchy2 as
	select brand_code
	,brand_description
	,product_key as Brand_Prod_Level_Key
	from takeda.takeda_product_hierarchy
	where product_level = "Brand";
quit; *The formulary table references only the product key for a product where the product level = "Brand"
For example, Trintellix product key is 116900-116906, where 01-05 are for different strengths,06 is for calls
and 116900 is considered the brand product level. This is the code that links to the Formulary table, not 01-06. 
As a result, I needed to put the brand product level key in with each product so that the formulary table 
can be joined in later on. The next step finishes the merge of the brand level to the product hierarchy.
Ask Corey for further clarification if needed;

proc sql;
	create table takeda.takeda_product_hierarchy as
	select *
	from takeda.takeda_product_hierarchy as pha
	left join product_hierarchy2 as phb
	on pha.Brand_code = phb.brand_code and pha.brand_description = phb.brand_description;
quit;

proc sql;
	create table takeda2.prodhierarchy_short as
	select product_key
	,brand_description
	,Brand_Prod_Level_Key
	,case when product_key in(112649,112650,112651,112437,112438,112439,112440,112441,118608,118609
	,112289,112290,112291,112292,112293) then "Generic"
		else LEGAL_CLASS
	end as LEGAL_CLASS
	,class_code
	from takeda.takeda_product_hierarchy;
quit; *6,374 observation, 2variables, instant run. ****The product hierarchy has Wellbutrin and Budeprion 
labeled as branded drugs, but they are no longer branded so this code also converts these drug's legal_class
from Branded to Generic;


proc sql;
	create table takeda2.plan_dimension_short as
	select distinct src_plan_key
	,input(substr(DATA_MONTH_DATE_KEY,1,6),6.) as month_key
	,payer_mix
	,plan_id
	,parent_name
	from takeda2.plan_dimension
	where src_plan_key > 0;
quit; *Shortens Dimension table to 54934 observations and 2 variables, 5 sec to run;


proc sql;
	create table takeda2.formulary_short as
	select distinct PLAN_ID
	,input(substr(DATA_MONTH_DATE_KEY,1,6),6.) as month_key
	,product_key
	,case when TIER = 1 then "Tier 1"
		when TIER = 2 then "Tier 2"
		when TIER = 3 then "Tier 3"
		when TIER = 4 then "Tier 4"
		when TIER = 10 then "Not Covered"
		else "Unknown Tier"
	 end as Tier
	from TAKEDA2.FORMULARYDRUGSUMMARY
	where product_key > 0;
quit; *Shortens formulary table. 2 min to run;


*********************************************************************************

Convert cns_claims table's month key from YYYYMMDD format to YYYYMM and maintain
only relevant variables and keys. Also creates Patient Out of Pocket (OOP) groupings.

*********************************************************************************;


proc sql;
	create table takeda.cns_claims as
	select input(substr(put(dspnd_mth_key,16. -L),1,6),6.) as dspnd_mth_key
	,clm_stus_cd
	,PTNT_AGE_RANGE
	,PTNT_GNDR_CD
	,REPORTING_PRODUCT_ID
	,PLAN_KEY
	,SEC_PLAN_KEY
	,CUST_KEY
	,CLAIM_ID
	,CLM_RJCT_TYP_KEY
	,CLM_DAW_KEY
	,case when missing(PTNT_TO_PAY_CLCTD_AMT) or  PTNT_TO_PAY_CLCTD_AMT = 0 then "0"
		when 0 < PTNT_TO_PAY_CLCTD_AMT < 5 then "0-5"
		when 5 <= PTNT_TO_PAY_CLCTD_AMT < 10 then "5-10"
		when 10 <= PTNT_TO_PAY_CLCTD_AMT < 15 then "10-15"
		when 15 <= PTNT_TO_PAY_CLCTD_AMT < 20 then "15-20"
		when 20 <= PTNT_TO_PAY_CLCTD_AMT < 25 then "20-25"
		when 25 <= PTNT_TO_PAY_CLCTD_AMT < 30 then "25-30"
		when 30 <= PTNT_TO_PAY_CLCTD_AMT < 35 then "30-35"
		when 35 <= PTNT_TO_PAY_CLCTD_AMT < 40 then "35-40"
		when 40 <= PTNT_TO_PAY_CLCTD_AMT < 45 then "40-45"
		when 45 <= PTNT_TO_PAY_CLCTD_AMT < 50 then "45-50"
		when 50 <= PTNT_TO_PAY_CLCTD_AMT < 55 then "50-55"
		when 55 <= PTNT_TO_PAY_CLCTD_AMT < 60 then "55-60"
		when 60 <= PTNT_TO_PAY_CLCTD_AMT < 65 then "60-65"
		when 65 <= PTNT_TO_PAY_CLCTD_AMT < 70 then "65-70"
		when 70 <= PTNT_TO_PAY_CLCTD_AMT < 75 then "70-75"
		when 75 <= PTNT_TO_PAY_CLCTD_AMT < 80 then "75-80"
		when 80 <= PTNT_TO_PAY_CLCTD_AMT < 85 then "80-85"
		when 85 <= PTNT_TO_PAY_CLCTD_AMT < 90 then "85-90"
		when 90 <= PTNT_TO_PAY_CLCTD_AMT < 95 then "90-95"
		when 95 <= PTNT_TO_PAY_CLCTD_AMT < 100 then "95-100"
		else "100+"
	end as OOP		
	from TAKEDA.fia_cns_claims;
quit; *3.4m obs, 12min to run;


*********************************************************************************

Data too large so break table down to smaller sets (month long) for improved processing speed;

*********************************************************************************;


proc sql;
	%do k=1 %to 24;
		create table takeda2.CNS&k as
		select *
		from takeda.cns_claims
		where case
			when &k=1 then dspnd_mth_key in(201710)
			when &k=2 then dspnd_mth_key in(201711)
			when &k=3 then dspnd_mth_key in(201712)
			when &k=4 then dspnd_mth_key in(201801)
			when &k=5 then dspnd_mth_key in(201802)
			when &k=6 then dspnd_mth_key in(201803)
			when &k=7 then dspnd_mth_key in(201804)
			when &k=8 then dspnd_mth_key in(201805)
			when &k=9 then dspnd_mth_key in(201806)
			when &k=10 then dspnd_mth_key in(201807)
			when &k=11 then dspnd_mth_key in(201808)
			when &k=12 then dspnd_mth_key in(201809)
			when &k=13 then dspnd_mth_key in(201810)
			when &k=14 then dspnd_mth_key in(201811)
			when &k=15 then dspnd_mth_key in(201812)
			when &k=16 then dspnd_mth_key in(201901)
			when &k=17 then dspnd_mth_key in(201902)
			when &k=18 then dspnd_mth_key in(201903)
			when &k=19 then dspnd_mth_key in(201904)
			when &k=20 then dspnd_mth_key in(201905)
			when &k=21 then dspnd_mth_key in(201906)
			when &k=22 then dspnd_mth_key in(201907)
			when &k=23 then dspnd_mth_key in(201908)
			when &k=24 then dspnd_mth_key in(201909)
			end;
	%end;
quit; *about 2.5+ min per table to run, 63.5min to run total;




*********************************************************************************

Join data sets

*********************************************************************************;


*******LEFT JOIN the product hierarchy table to the CNS Data;
proc sql;
	%do k=1 %to 24;
		create table takeda2.CNSProdJoin&k as
		select *
		from takeda2.CNS&k as cns
		Left Join takeda2.prodhierarchy_short as ph
		on cns.reporting_product_id = ph.product_key;
	%end;
quit; *1 min each - 24 min total, table 1 has 14,419,233 observations
Joins the prodhierarchy to CNS data and then the plan dimension data to that new table;



******LEFT JOIN plan dimension;
proc sql;
	%do k=1 %to 24;
		create table takeda2.CNSPlanDimJoin&k as
		select * 
		from takeda2.CNSProdJoin&k as cpj
		left join TAKEDA2.PLAN_DIMENSION_short as pd
		on cpj.plan_key = pd.SRC_PLAN_KEY
		and cpj.dspnd_mth_key=pd.month_key;
	%end;
quit;*good, table 1 has 14,419,233 observations, run time: 2min per table, 48min total;



********LEFT JOIN formulary;
proc sql;
	%do k=1 %to 24;
		create table takeda2.cnsformularyjoin&k as
		select * 
		from takeda2.CNSPlanDimJoin&k as pj
		left join takeda2.formulary_short as fs
		on pj.plan_id=fs.plan_id 
		and pj.Brand_Prod_Level_Key=fs.product_key 
		and pj.dspnd_mth_key=fs.month_key;
	%end;
quit; *table 1 has 14,419,233 observations. run time: 2.8min each - 67 min total;



*********************************************************************************

Create aggregates of the variables to be copied over to excel and used in a pivot table;

*********************************************************************************;

proc sql;
	%do k=1 %to 24;
		create table takeda2.cnsaggregates&k as
		Select dspnd_mth_key as Month
		,case when dspnd_mth_key < 201810 then "MAT18"
			else "MAT19" 
		 end as MAT
		,brand_description as Brand
		,legal_class as Brand_or_Generic
		,class_code as Drug_Class
		,case when clm_stus_cd = "A" then "Paid"
			when clm_stus_cd = "P" then "Reversed"
			when clm_stus_cd = "R" then "Rejected" 
			else "" 
		 end as Status
		,case when PAYER_MIX = "Commercial" then "Commercial"
			when Payer_Mix = "Part D" then "Medicare"
			when Payer_Mix = "Managed Medicaid" then "Medicaid"
			when Payer_Mix = "FFS Medicaid" then "Medicaid"
			else "Other" 
		 end as Payer_Type
		,tier as Tier
		,oop as Patient_OOP
		,parent_name as Plan_Name
		,count(*) as Claims
		from takeda2.cnsformularyjoin&k
		where legal_class="Branded"
		group by dspnd_mth_key
		,MAT
		,brand_description
		,legal_class
		,class_code
		,Status
		,PAYER_Type
		,tier
		,oop
		,parent_name;
	%end; 
quit; *30,628k obs in first table, <1 min to run each, 24 min total;






	
