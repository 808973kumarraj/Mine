#!/bin/bash
################################################################################
##
## THIS SCRIPT WILL CHECK LOAD FROM 20TH OF LAST MONTH TO CURRENT MISDATE IN MONTHLY DRI TABLES AND FAIL IF NO LOAD HAS HAPPENED
##
## Version: 2.1.0 - Fixed continuous holiday handling issue
## Author: ASPAC Data Services Team
## Last Modified: June 2025
##
################################################################################

# Input parameters
env=${1^^}
cntry_cde=$2
curr_date=`date +'%Y%m%d'`

# Configuration
country_code='russia'
base_dir='/data/1/`whoami`/bin/$country_code/etl'
parentPath=${base_dir}/scripts/
log_dir=${base_dir}/logs

# Database configuration
config_file=${base_dir}/config/emea_dm_ru_config.txt
l1_schema=`cat $config_file | grep L1_SCHEMA_NAME | cut -d "=" -f2`
stg_schema=`cat $config_file | grep STG_SCHEMA_NAME | cut -d "=" -f2`
il_schema=`cat $config_file | grep IL_SCHEMA_NAME | cut -d "=" -f2`
prgm='chk_mly_load'

# Initialize logging
driver_log_file=${log_dir}/driver_${prgm}_${curr_date}.log
> $driver_log_file
exec > >(tee -a ${driver_log_file} >&2 )
exec 2> >(tee -a ${driver_log_file} >&2 )

# Email configuration - UPDATE THIS WITH YOUR EMAIL ADDRESSES
EMAIL=""

# Initialize Kerberos authentication
source /data/1/${whoami}/bin/kinit_source/kinit_source.sh $env

# Email notification function
SendEmail() {
    event_code=$1
    message=$2
    if [ $event_code -eq 0 ] ; then
        echo -e "$message" | mailx -s "AML APAC DATAMART ${cntry_cde} ${prgm} SUCCESSFUL FOR ${curr_date}" $EMAIL
    else
        echo -e "$message" | mailx -s "AML APAC DATAMART ${cntry_cde} ${prgm} FAILED FOR ${curr_date}" $EMAIL
    fi
}

# Global variables
check_flag=""

# Function to check if record exists in eap_batchrun
check_record() {
    prgm=$1
    check_query="select count(*) from ${stg_schema}.eap_batchrun where eap_country_code = '${cntry_cde}' and mis_date = '${curr_date}'"
    echo ""
    echo "`date +'%Y-%m-%d %H:%M:%S'` QUERYING EAP_BATCHRUN TO CHECK IF RECORD FOR ${curr_date} ALREADY EXISTS"
    
    ac=`beeline -u "${url}" --showHeader=false --silent=true -e "${check_query}"`
    if [ $? -ne 0 ] ; then
        echo ""
        echo "`date +'%Y-%m-%d %H:%M:%S'` FAILED TO CONNECT TO BEELINE"
        exit 1
    else
        echo ""
        echo "`date +'%Y-%m-%d %H:%M:%S'` BEELINE Connection SUCCESSFUL"
    fi
    
    check_flag=$(echo "$ac" | tr -dc '0-9')
    echo ""
    echo "`date +'%Y-%m-%d %H:%M:%S'` RECORD COUNT FROM EAP_BATCHRUN TABLE FOR ${curr_date} & ${cntry_cde} FOR ${prgm} is ${check_flag}"
}

# Initialize application logger
logger_log_file=${log_dir}/logger_${prgm}_${country_code}_${curr_date}.log
> $logger_log_file

# Main processing logic
if [ $prgm == 'chk_mly_load' ] ; then
    
    # Get the earliest unprocessed date
    mis_date_temp_1=`beeline -u "$url" --showHeader=false --outputformat=csv2 -e "select date_format(from_unixtime(unix_timestamp(min(mis_date),'yyyyMMdd')),'yyyy-MM-dd') from ${stg_schema}.eap_batchrun where eap_country_code = '${cntry_cde}' and trim(status)='N'"`

    if [ "${mis_date_temp_1,,}" == "null" ] ; then
        echo "`date +'%Y-%m-%d %H:%M:%S'` There are no entries in eap_batchrun with status 'N'. Exiting..."
        exit 1
    fi

    # CRITICAL FIX: Get work_day and status information for holiday detection
    # - Removed trim(status)='Y' filter to include holiday records 
    # - Added date format conversion using replace() to match database format
    # - eap_batchrun stores dates as YYYYMMDD, script uses YYYY-MM-DD
    mis_date_temp=`beeline -u "$url" --showHeader=false --outputformat=csv2 -e "select work_day,status from ${stg_schema}.eap_batchrun where eap_country_code = '${cntry_cde}' and mis_date = replace('${mis_date_temp_1}','-','')"`
    
    # Extract work_day and status from query result
    work_day=`echo $mis_date_temp | cut -d ',' -f 1| xargs`
    status=`echo $mis_date_temp | cut -d ',' -f 2| xargs`

    # Holiday detection logic
    if [ "$work_day" == "H" ] && [ "$status" == "N" ] ; then
        mis_date="NULL"
    else
        mis_date=$mis_date_temp_1
    fi

    echo "The mis_date is $mis_date"
    echo "`date +'%Y-%m-%d %H:%M:%S'` The mis_date is $mis_date">>$logger_log_file

    # Handle holiday scenario
    if [ "${mis_date,,}" == "null" ] ; then
        # Get the most recent processed date for holiday handling
        mis_date_hol=`beeline -u "$url" --showHeader=false --outputformat=csv2 -e "select date_format(from_unixtime(unix_timestamp(max(mis_date),'yyyyMMdd')),'yyyy-MM-dd') from ${stg_schema}.eap_batchrun where eap_country_code = '${cntry_cde}' and trim(status)='Y'"`
        
        ## Handle current holiday run
        mis_date=$mis_date_hol
        echo "`date +'%Y-%m-%d %H:%M:%S'` The holiday mis_date is $mis_date">>$logger_log_file
    fi

    # Calculate start date (20th of previous month)
    start_date=`beeline -u "$url" --showHeader=false --outputformat=csv2 -e "select date_format(date_add(last_day(add_months('${mis_date}',-2)),20),'yyyy-MM-dd')"`
    echo "The load is being checked from ${start_date} until ${mis_date}"
    echo "`date +'%Y-%m-%d %H:%M:%S'` The load is being checked from ${start_date} until ${mis_date}">>$logger_log_file
    
    # Get load month for reporting
    load_month=`beeline -u "$url" --showHeader=false --outputformat=csv2 -e "select date_format('${start_date}','MM')"`

    # Check if all 4 DRI tables have monthly loads
    rec_cnt=`beeline -u "$url" --showHeader=false --outputformat=csv2 -e "select sum(cnt) as sumcnt from (
select 1 as cnt from ${il_schema}.dri_rm_cust_mly where mis_date > '${start_date}' and mis_date <= '${mis_date}' and eap_country_code = '${cntry_cde}' group by month(mis_date) 
union all
select 1 as cnt from ${il_schema}.dri_rm_addr_mly where mis_date > '${start_date}' and mis_date <= '${mis_date}' and eap_country_code = '${cntry_cde}' group by month(mis_date)
union all  
select 1 as cnt from ${il_schema}.dri_rm_reln_mly where mis_date > '${start_date}' and mis_date <= '${mis_date}' and eap_country_code = '${cntry_cde}' group by month(mis_date)
union all
select 1 as cnt from ${il_schema}.dri_rm_tel_mly where mis_date > '${start_date}' and mis_date <= '${mis_date}' and eap_country_code = '${cntry_cde}' group by month(mis_date)
)"` 

    # Initialize counters
    not_loaded=0
    not_loaded_any=0
    not_load_tables=""

    # Process results
    if [ "${rec_cnt,,}" == "null" ] ; then
        echo "The total monthly tables loaded for the month ${load_month} is ${rec_cnt}"
        echo "`date +'%Y-%m-%d %H:%M:%S'` The total monthly tables loaded until ${mis_date} is ${rec_cnt}" >>$logger_log_file
        echo ""
        echo "                                    LOAD REPORT                               " >>$logger_log_file
        echo "=============================================================================" >>$logger_log_file
        
        # Check each DRI table individually
        for i in dri_rm_reln_mly dri_rm_cust_mly dri_rm_addr_mly dri_rm_tel_mly
        do
            table=$i
            
            # Get load dates for this table
            load_date=`beeline -u "$url" --showHeader=false --outputformat=csv2 -e "select mis_date from ${il_schema}.$i where mis_date > '${start_date}' and mis_date <= '${mis_date}' and eap_country_code = '${cntry_cde}' group by mis_date | tr ',' ','"`
            
            # Count number of months loaded
            load_mths_cnt=`echo ${load_date} | tr ',' '\n' | cut -d '-' -f 1,2 | sort | uniq | xargs -I {} date -d '{}-01' '+%b %c' | wc -l`

            if [ ${load_mths_cnt} -eq 1 ] ; then
                load_months=`echo ${load_date} | tr ',' '\n' | cut -d '-' -f 1,2 | sort | uniq | xargs -I {} date -d '{}-01' '+%b %c'`
            else
                load_months=`echo ${load_date} | tr ',' '\n' | cut -d '-' -f 1,2 | sort | uniq | xargs -I {} date -d '{}-01' '+%b %c' | tr '\n' ',' | cut -d ',' -f 1,2`
            fi

            if [ -z "${load_date}" ] || [ "${load_date}" == "" ] ; then
                status="NOT LOADED"
                echo "Table $i was not loaded with monthly files for ${load_month}"
                printf "`date +'%Y-%m-%d %H:%M:%S'` | %-15s | %-10s | for %-10s | %-20s \n" "${table}" "${status}" "${load_months}" "${load_date}" >>$logger_log_file
                not_load_tables="${not_load_tables} $i"
                not_loaded=1
            else
                status="loaded"
                echo "Table $i was loaded on ${load_date} for the month of ${load_months}"
                printf "`date +'%Y-%m-%d %H:%M:%S'` | %-15s | %-10s | for %-10s | %-20s \n" "${table}" "${status}" "${load_months}" "${load_date}" >>$logger_log_file
            fi
        done
    else
        if [ "${rec_cnt,,}" == "null" ] ; then
            not_loaded_any=1
        fi
    fi

    # Final status check and exit
    if [ ${not_loaded_any} -ne 0 ] ; then
        echo "Monthly files not loaded for any of the 4 monthly DRI tables for the month of ${load_month}."
        echo "`date +'%Y-%m-%d %H:%M:%S'` Monthly files not loaded for any of the 4 monthly DRI tables from ${start_date} until ${mis_date}" >>$logger_log_file
        
        # Send failure notification
        failure_message="Monthly DRI table load validation failed.
Environment: $env
Country: $cntry_cde  
Date Range: $start_date to $mis_date
Issue: No monthly tables loaded
Check logs: $logger_log_file"
        
        SendEmail 1 "$failure_message"
        exit 1
    else
        if [ ${not_loaded} -ne 0 ] ; then
            echo "The monthly feeds are not loaded for the month ${load_month} in ${not_load_tables}"
            echo "`date +'%Y-%m-%d %H:%M:%S'` Warning !!! Please check if monthly load was done for ${load_month}" >>$logger_log_file
            
            # Send warning notification
            warning_message="Monthly DRI table load validation warning.
Environment: $env
Country: $cntry_cde
Date Range: $start_date to $mis_date
Missing Tables: $not_load_tables
Check logs: $logger_log_file"
            
            SendEmail 1 "$warning_message"
            exit 1
        else
            echo "The monthly feeds are loaded for the month ${load_month}"
            echo "`date +'%Y-%m-%d %H:%M:%S'` The monthly feeds are loaded for the month ${load_month}" >>$logger_log_file
            
            # Send success notification
            success_message="Monthly DRI table load validation completed successfully.
Environment: $env
Country: $cntry_cde
Date Range: $start_date to $mis_date
Status: All 4 monthly tables loaded successfully
Tables: dri_rm_cust_mly, dri_rm_addr_mly, dri_rm_reln_mly, dri_rm_tel_mly"
            
            SendEmail 0 "$success_message"
        fi
    fi
fi

echo "`date +'%Y-%m-%d %H:%M:%S'` Monthly load check completed successfully" >>$logger_log_file
exit 0