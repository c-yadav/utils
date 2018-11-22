#!/bin/ksh

#
##	@cy, 0.1, JUL/14
##
##	Pre-Requisites:
##	1. User Equivalence between source and target host
##	2. ksh93
#

if [[ $# -ne 3 ]]
then
        echo "usage : $0 FILE_HAVING_LIST TARGET_IP TARGET_PATH"
        exit 1
fi

## GLOBAL VARs#
typeset -A BG_PROCTAB
typeset -A BG_ERRLOG
RUNNING_PROCESSES=0
PWD=$(pwd)
PROCESS_ID=${$}
FILE_HAVING_LIST=$1
TARGET_IP=$2
TARGET_PATH=$3
PARALLEL_MAX_PROCESS=20         ## controls no. of max. concurrent COPY process
TIME_BETWEEN_BGPROC_UPDATE=60   ## controls sleep time between background processes updates
TIME_BETWEEN_INPUT_UPDATE=120   ## controls sleep time between input ( new files to process ) updates
TIME_TO_LOGOUT=120              ## controls time for the user to logout before the script starts execution
ERRLOG_COUNT=0                  ## used for creating each BG process's error log
DYANAMIC_VAR_FILE=${PWD}/.dyanamic_vars.lst
SCRIPT_LOG=${PWD}/${0}.log.${PROCESS_ID}
ERROR_LOG=${PWD}/${0}.err.${PROCESS_ID}
LAST_FILE=${PWD}/.lastfile.lst.${PROCESS_ID}
CURRENT_FILE=${PWD}/.currentfile.lst.${PROCESS_ID}
HISTORY_FILE=${PWD}/.history.$(echo ${FILE_HAVING_LIST}|sed 's#/#_#g').lst

SUB_UPDATE_DYNAMIC_VAR()
## helper function for function SUB_UPDATE_DYNAMIC_VARS
{
        ## args : dyanamic parameter name
        typeset PARAM_NAME=$1
        eval OLD_PARAM_VALUE=\$${PARAM_NAME}
        typeset NEW_PARAM_VALUE=`cat ${DYANAMIC_VAR_FILE}|eval grep -w ^${PARAM_NAME}|cut -d '|' -f 2`
        if [[ -z ${NEW_PARAM_VALUE} ]]
        then
                eval ${PARAM_NAME}=${OLD_PARAM_VALUE}
                echo "${PARAM_NAME}|${OLD_PARAM_VALUE}" >> ${DYANAMIC_VAR_FILE}
                echo "Time:`date +%d%b%y-%HH%MM%SS`|INFO| dyanamic variable ${PARAM_NAME} absent in file ${DYANAMIC_VAR_FILE}, defaults to -> ${OLD_PARAM_VALUE}, entry added in file." | tee -a $SCRIPT_LOG
        fi
        if [[ ${NEW_PARAM_VALUE} -ne ${OLD_PARAM_VALUE} ]]
        then
                eval ${PARAM_NAME}=${NEW_PARAM_VALUE}
                echo "Time:`date +%d%b%y-%HH%MM%SS`|INFO| dyanamic variable ${PARAM_NAME} value changed from ${OLD_PARAM_VALUE} to ${NEW_PARAM_VALUE}" | tee -a $SCRIPT_LOG
        fi
}

SUB_UPDATE_DYNAMIC_VARS()
## updates variable dynamic variables value from file $DYANAMIC_VAR_FILE, thus making them dyanamic.
## contents of .dyanamic_vars.lst looks like (below shows default values, values are in seconds):
##PARALLEL_MAX_PROCESS|20
##TIME_BETWEEN_BGPROC_UPDATE|60
##TIME_BETWEEN_INPUT_UPDATE|120
##TIME_TO_LOGOUT|20
{
        if [[ ! -e ${DYANAMIC_VAR_FILE} ]]
        then
                echo "PARALLEL_MAX_PROCESS|${PARALLEL_MAX_PROCESS}" > ${DYANAMIC_VAR_FILE}
                echo "TIME_BETWEEN_BGPROC_UPDATE|${TIME_BETWEEN_BGPROC_UPDATE}" >> ${DYANAMIC_VAR_FILE}
                echo "TIME_BETWEEN_INPUT_UPDATE|${TIME_BETWEEN_INPUT_UPDATE}" >> ${DYANAMIC_VAR_FILE}
                echo "TIME_TO_LOGOUT|${TIME_TO_LOGOUT}" >> ${DYANAMIC_VAR_FILE}
                echo "Time:`date +%d%b%y-%HH%MM%SS`|INFO|${DYANAMIC_VAR_FILE} absent. PARALLEL_MAX_PROCESS defaults to --> ${PARALLEL_MAX_PROCESS}" | tee -a $SCRIPT_LOG
                echo "Time:`date +%d%b%y-%HH%MM%SS`|INFO|${DYANAMIC_VAR_FILE} absent. TIME_BETWEEN_BGPROC_UPDATE defaults to --> ${TIME_BETWEEN_BGPROC_UPDATE}" | tee -a $SCRIPT_LOG
                echo "Time:`date +%d%b%y-%HH%MM%SS`|INFO|${DYANAMIC_VAR_FILE} absent. TIME_BETWEEN_INPUT_UPDATE defaults to --> ${TIME_BETWEEN_INPUT_UPDATE}" | tee -a $SCRIPT_LOG
                echo "Time:`date +%d%b%y-%HH%MM%SS`|INFO|${DYANAMIC_VAR_FILE} absent. TIME_TO_LOGOUT defaults to --> ${TIME_TO_LOGOUT}" | tee -a $SCRIPT_LOG
        else
                SUB_UPDATE_DYNAMIC_VAR PARALLEL_MAX_PROCESS
                SUB_UPDATE_DYNAMIC_VAR TIME_BETWEEN_BGPROC_UPDATE
                SUB_UPDATE_DYNAMIC_VAR TIME_BETWEEN_INPUT_UPDATE
                SUB_UPDATE_DYNAMIC_VAR TIME_TO_LOGOUT
        fi
}

SUB_UPDATE_RUNNING_PROCESSES()
## update running BG processes count.
{
        typeset BG_PROCTAB_KEYS=(${!BG_PROCTAB[@]})     # indexed array
        RUNNING_PROCESSES=${#BG_PROCTAB_KEYS[@]}
}

SUB_PRINT_PROCESS_DETAILS()
{
## print background process details to log $SCRIPT_LOG, when it starts or ends.
        ## passed args: child-process-id, start or end
        typeset CHILD_PROCESS=$1
        typeset CHILD_PROCESS_STATE=$2
        echo "Time:`date +%d%b%y-%HH%MM%SS`|Parent Process ${$}| Child Process ${CHILD_PROCESS} |FILE-> ${BG_PROCTAB["${CHILD_PROCESS}"]} -> ${CHILD_PROCESS_STATE}" | tee -a $SCRIPT_LOG

}

SUB_WRITE_TO_HISTORY_FILE()
## write filename to history file post BG process end
{
        ## args : child-process-id
        echo "${BG_PROCTAB["${CHILD_PROCESS}"]}" >> ${HISTORY_FILE}
}

SUB_ADD_BG_PROCESS()
## add new background process details to BG_PROCTAB assosciative array(hash).
{
        ## passed args : filename, child-process-id
        typeset FILE=$1
        typeset CHILD_PROCESS=$2
        BG_PROCTAB["${CHILD_PROCESS}"]="${FILE}"
        SUB_PRINT_PROCESS_DETAILS "$CHILD_PROCESS" "START"
        SUB_UPDATE_RUNNING_PROCESSES
}

SUB_UPDATE_COMMON_ERRLOG_SCRIPTLOG_HISTORY()
{
        ## args : child-process
        typeset BGPROC_OWN_ERRLOG="${BG_ERRLOG["${CHILD_PROCESS}"]}"
        typeset NUM_ERROR_LINES=`cat ${BGPROC_OWN_ERRLOG}|grep -v ^#|wc -l`
        if [[ ${NUM_ERROR_LINES} -ne 0 ]]
        then
                ## if NUM_ERROR_LINES not equals 0, then errors present
                echo "#${BG_PROCTAB["${CHILD_PROCESS}"]}|ERRORS:YES|error details:" >> ${ERROR_LOG}
                cat ${BGPROC_OWN_ERRLOG}|grep -v ^# >> ${ERROR_LOG}
                SUB_PRINT_PROCESS_DETAILS ${CHILD_PROCESS} "END-ERRORS-SEE-${ERROR_LOG}"
                rm ${BGPROC_OWN_ERRLOG}
        else
                SUB_PRINT_PROCESS_DETAILS ${CHILD_PROCESS} "END-NOERRORS"
                SUB_WRITE_TO_HISTORY_FILE ${CHILD_PROCESS}
                rm ${BGPROC_OWN_ERRLOG}
        fi
}

SUB_UPDATE_BG_PROCESSES()
## update background process details to BG_PROCTAB, like deleting completed BG processes details --
## from BG_PROCTAB and printing details to $SCRIPT_LOG
{
        for CHILD_PROCESS in "${!BG_PROCTAB[@]}"
        do
                kill -0 ${CHILD_PROCESS} 1>/dev/null 2>&1
                ## if $? is not equal to 0, then process doesn't exist
                if [[ $? -ne 0 ]]
                then
                        ## print to log details of child process and delete entry from BG_PROCTAB
                        #SUB_PRINT_PROCESS_DETAILS ${CHILD_PROCESS} "END"
                        #SUB_WRITE_TO_HISTORY_FILE ${CHILD_PROCESS}
                        SUB_UPDATE_COMMON_ERRLOG_SCRIPTLOG_HISTORY
                        unset BG_PROCTAB["${CHILD_PROCESS}"]
                fi
        done
        SUB_UPDATE_RUNNING_PROCESSES
        echo "Time:`date +%d%b%y-%HH%MM%SS`|Parent Process ${$}| BGPROC_TAB update run @interval of ${TIME_BETWEEN_BGPROC_UPDATE} | running BG process : ${RUNNING_PROCESSES}, max allowed :${PARALLEL_MAX_PROCESS} "| tee -a $SCRIPT_LOG
}

SUB_WAIT_FOR_BGPROC_FALL_BELOW_MAX_PARALLEL()
{
        SUB_UPDATE_RUNNING_PROCESSES
        SUB_UPDATE_DYNAMIC_VARS
        while [[ ${RUNNING_PROCESSES} -ge ${PARALLEL_MAX_PROCESS} ]]
        do
                sleep ${TIME_BETWEEN_BGPROC_UPDATE}
                SUB_UPDATE_BG_PROCESSES
                SUB_UPDATE_DYNAMIC_VARS
        done
}

CHECK_FILE_PRESENT_IN_HIST()
{
        if [[ $# -ne 1 ]]
        then
                echo "CODE ERR| SYNTAX to use this function : $0 <<FILE NAME>>, where FILE NAME will be checked in history file for existence."
                exit 2
        fi

        IS_FILE_PRESENT_IN_HIST=`cat ${HISTORY_FILE}|grep -w $FL|wc -l`
        return ${IS_FILE_PRESENT_IN_HIST}
}

## -- below function, discontinued,
#PRINT_SCPED_FILES()
#{
#       if [[ $# -ne 1 ]]
#       then
#               echo "CODE ERR| SYNTAX to use this function : $0 TRUE/FALSE, where TRUE/FALSE is for whether history file present or absent"
#               exit 1
#       fi
#
#       HIST_FILE_PRESENT=`echo $1|tr '[a-z]' '[A-Z]'`
#       for FL in ` diff ${LAST_FILE} ${CURRENT_FILE}|grep ^">"|awk '{ print $2 }'`
#       do
#               if [[ ${HIST_FILE_PRESENT} == "TRUE" ]]
#               then
#                       IS_FILE_PRESENT_IN_HIST=`cat ${HISTORY_FILE}|grep -w $FL|wc -l`
#                       if [[ ${IS_FILE_PRESENT_IN_HIST} -eq 0 ]]
#                       then
#                               echo "Time:`date +%d%b%y-%HH%MM%SS`|Parent Process ${$}|SCP of file ${FL} Completed."| tee -a $SCRIPT_LOG
#                               echo ${FL} >> ${HISTORY_FILE}
#                       else
#                               echo "Time:`date +%d%b%y-%HH%MM%SS`|${FL} present in history file."| tee -a $SCRIPT_LOG
#                       fi
#               fi
#
#               if [[ ${HIST_FILE_PRESENT} == "FALSE" ]]
#               then
#                       echo "Time:`date +%d%b%y-%HH%MM%SS`|Parent Process ${$}|SCP of file ${FL} Completed."| tee -a $SCRIPT_LOG
#                       echo ${FL} >> ${HISTORY_FILE}
#               fi
#       done
#}

SUB_INC_ERRLOG_COUNT()
{
        ERRLOG_COUNT=`expr ${ERRLOG_COUNT} + 1`
}

SUB_ADD_BG_PROC_ERRORLOG()
{
        ## passed args : errorlog, child-process-id
        typeset BGPROC_OWN_ERRLOG=$1
        typeset CHILD_PROCESS=$2
        BG_ERRLOG["${CHILD_PROCESS}"]="${BGPROC_OWN_ERRLOG}"
}

SUB_SCP_FILE()
{
        if [[ $# -ne 1 ]]
        then
                echo "CODE ERR| SYNTAX to use this function : $0 TRUE/FALSE, where TRUE/FALSE is for whether history file present or absent"
                exit 1
        fi

        typeset HIST_FILE_PRESENT=`echo $1|tr '[a-z]' '[A-Z]'`

        for FL in `diff ${LAST_FILE} ${CURRENT_FILE}|grep ^">"|awk '{ print $2 }'`
        do
                if [[ ${HIST_FILE_PRESENT} == "TRUE" ]]
                then
                        IS_FILE_PRESENT_IN_HIST=`cat ${HISTORY_FILE}|grep -w $FL|wc -l`
                        if [[ ${IS_FILE_PRESENT_IN_HIST} -eq 0 ]]
                        then
                                SUB_UPDATE_DYNAMIC_VARS
                                ## SUB_WAIT_FOR_BGPROC_FALL_BELOW_MAX_PARALLEL function waits from running BG processes to fall below max parallel
                                SUB_WAIT_FOR_BGPROC_FALL_BELOW_MAX_PARALLEL
                                SUB_INC_ERRLOG_COUNT
                                typeset BGPROC_OWN_ERRLOG="${PWD}/.error.log.${ERRLOG_COUNT}"
                                exec scp ${FL} ${TARGET_IP}:${TARGET_PATH} 1>/dev/null 2>${BGPROC_OWN_ERRLOG} &
                                SUB_ADD_BG_PROCESS ${FL} $!
                                SUB_ADD_BG_PROC_ERRORLOG ${BGPROC_OWN_ERRLOG} $!
                        else
                                echo "Time:`date +%d%b%y-%HH%MM%SS`|Parent Process ${$}|FILE-> $FL skipped, present in history file."| tee -a $SCRIPT_LOG
                        fi
                fi
                if [[ ${HIST_FILE_PRESENT} == "FALSE" ]]
                then
                        SUB_UPDATE_DYNAMIC_VARS
                        ## SUB_WAIT_FOR_BGPROC_FALL_BELOW_MAX_PARALLEL function waits from running BG processes to fall below max parallel
                        SUB_WAIT_FOR_BGPROC_FALL_BELOW_MAX_PARALLEL
                        SUB_INC_ERRLOG_COUNT
                        typeset BGPROC_OWN_ERRLOG="${PWD}/.error.log.${ERRLOG_COUNT}"
                        exec scp ${FL} ${TARGET_IP}:${TARGET_PATH} 1>/dev/null 2>${BGPROC_OWN_ERRLOG} &
                        SUB_ADD_BG_PROCESS ${FL} $!
                        SUB_ADD_BG_PROC_ERRORLOG ${BGPROC_OWN_ERRLOG} $!
                fi
        done
        ## as all of difference between current and last ones have been submitted,--
        ##-- now,wait for 'em to end, post that update BG_PROCTAB and update running BG processes
        wait
        SUB_UPDATE_BG_PROCESSES
}

MAIN()
{
		# get list of files to scp
        cat ${FILE_HAVING_LIST} > ${CURRENT_FILE}

        ## update running BG processes count
        SUB_UPDATE_RUNNING_PROCESSES

        if [[ -e ${HISTORY_FILE} ]]
        then
                HIST_FILE_PRESENT="TRUE"
        else
                HIST_FILE_PRESENT="FALSE"
        fi

        if [[ ! -e $LAST_FILE ]]
        then
                echo "Time:`date +%d%b%y-%HH%MM%SS`|Process ID: ${$}|history file exists: ${HIST_FILE_PRESENT} for ${FILE_HAVING_LIST}" | tee -a $SCRIPT_LOG
                cat /dev/null > ${LAST_FILE}
                SUB_SCP_FILE ${HIST_FILE_PRESENT}
                ##SUB_PRINT_COPIED_FILES ${HIST_FILE_PRESENT}   ##discontinued
                cp $CURRENT_FILE $LAST_FILE
        else
                FILE_DIFF=`diff ${LAST_FILE} ${CURRENT_FILE}|wc -l`
                if [[ ${FILE_DIFF} -eq 0 ]]
                then
                        echo "Time:`date +%d%b%y-%HH%MM%SS`|Process ID: ${$}|no files to process. Sleeping for ${TIME_BETWEEN_INPUT_UPDATE} s."| tee -a $SCRIPT_LOG
                        sleep ${TIME_BETWEEN_INPUT_UPDATE}
                        echo "Time:`date +%d%b%y-%HH%MM%SS`|Process ID: ${$}|Wakeup."| tee -a $SCRIPT_LOG
                else
                        SUB_SCP_FILE ${HIST_FILE_PRESENT}
                        ##SUB_PRINT_COPIED_FILES ${HIST_FILE_PRESENT}   ##discontinued
                        cp $CURRENT_FILE $LAST_FILE
                fi
        fi
}

SUB_UPDATE_DYNAMIC_VARS
SUB_UPDATE_RUNNING_PROCESSES

## CALL MAIN
echo "Sleep ${TIME_TO_LOGOUT} Seconds|--time to logout--|log --> ${SCRIPT_LOG}|parallelism --> ${PARALLEL_MAX_PROCESS}" | tee $SCRIPT_LOG
sleep ${TIME_TO_LOGOUT}
while true
do
        MAIN
done
