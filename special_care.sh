#!/bin/bash

# SpecialCare adjusts environment specific configuration values under a streaming ingestion directory.
#
# Expected globals before invocation:
#   RPM_ENVIRONMENT   - target environment name (e.g. DEV, SIT, UAT, PROD)
#   streamingestion_dir - base directory containing ingestion components
#   array             - list of subdirectories under streamingestion_dir to process
#
# The function replaces default DEV hostnames with their environment-specific
# counterparts and ensures each file has the correct "environment=" value set.
SpecialCare() {
    local env_lower="${RPM_ENVIRONMENT,,}"
    local Kerb_Hive_Host=""
    local RunBook_Server=""
    local environment_tag="DEV"

    case "$env_lower" in
        dev)
            Kerb_Hive_Host="bigdataplatform-nam-ot-dev2.nam.nsroot.net"
            RunBook_Server="namotdev2mysqlvip.nam.nsroot.net"
            environment_tag="DEV"
            ;;
        sit)
            Kerb_Hive_Host="bigdataplatform-hk-uat.apac.nsroot.net"
            RunBook_Server="eotuatapacmysqlvip.apac.nsroot.net"
            environment_tag="SIT"
            ;;
        uat)
            Kerb_Hive_Host="bigdataplatform-hk-uat.apac.nsroot.net"
            RunBook_Server="eotuatapacmysqlvip.apac.nsroot.net"
            environment_tag="UAT"
            ;;
        prod)
            Kerb_Hive_Host="bigdataplatform-sg.apac.nsroot.net"
            RunBook_Server="eotprodapacmysqlvip.apac.nsroot.net"
            environment_tag="PROD"
            ;;
        *)
            echo "Unsupported RPM environment: ${RPM_ENVIRONMENT}" >&2
            return 1
            ;;
    esac

    if [ -z "${array[*]}" ]; then
        echo "SpecialCare: no target components provided in array" >&2
        return 1
    fi

    if [ -z "$streamingestion_dir" ]; then
        echo "SpecialCare: streamingestion_dir is not set" >&2
        return 1
    fi

    for element in "${array[@]}"; do
        local target_dir="${streamingestion_dir}/$element"
        if [ ! -d "$target_dir" ]; then
            echo "SpecialCare: skipping missing directory $target_dir" >&2
            continue
        fi

        find "$target_dir" -type f -exec sed -i \
            -e "s,bigdataplatform-nam-ot-dev2.nam.nsroot.net,${Kerb_Hive_Host},g" \
            -e "s,namotdev2mysqlvip.nam.nsroot.net,${RunBook_Server},g" \
            -e "s,environment=[A-Za-z0-9_-]*,environment=${environment_tag},g" {} \;
    done
}
