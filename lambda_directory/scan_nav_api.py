import json



from lambda_directory import aurora_client

from lambda_directory import queries

import pandas as pd

from lambda_directory import constants

import humps

from lambda_directory import utils

import numpy as np

from copy import deepcopy



# from lambda_directory import postgre_client as postgre



# platform_type = 'ONELAKE_S3'





def get_all_cases(env, env_db, method, header, request_body={}, pathparams={}):

    if method == 'POST':

        try:

            print("Entered Search Cases API POST Method")

            admin_query_object = queries.SearchQueries()

            user_query_object = queries.NonAdminSearchQueries()

            conn = aurora_client.get_db_connection(env, env_db)

            # conn = postgre.create_connection("PROD")

            print("DB Connection made successfully!")



            search_filters = dict(request_body)



            if "limit" in search_filters.keys():

                limit = search_filters["limit"]

            else:

                limit = 1500



            if "offset" in search_filters.keys():

                offset = search_filters["offset"]

            else:

                offset = 0



            print("Getting User Role:")

            user_role = utils.get_user_role(header)

            # user_role = constants.admin

            print(user_role)

            user_case_id_tuple = utils.convert_list_to_sql_query_tuple([])



            if user_role != constants.admin:

                if user_role == constants.delegate:

                    # if role is PDS delegate

                    # Get EID of all PDS he is tagged under

                    # Check the case user table and get all cases assigned to the PDS EIDs

                    user_id = utils.get_user_eid(header)

                    # user_id = "NWP487"

                    user_case_id_tuple = get_non_admin_cases(user_id, conn)

                    print(user_case_id_tuple)



                else:

                    print("Getting User EID:")

                    user_id = utils.get_user_eid(header)

                    # user_id = "LDM860"

                    print(user_id)

                    user_cases_query = user_query_object.get_user_specific_cases(user_id)

                    print("Getting USER CASES DF")

                    user_cases_df = pd.read_sql_query(user_cases_query, conn)

                    print(user_cases_df)

                    user_case_id_tuple = utils.convert_list_to_sql_query_tuple(list(user_cases_df["case_id"]))



            if user_role == constants.admin:

                get_cases_query = admin_query_object.get_search_cases_query(limit, offset)

            else:

                get_cases_query = user_query_object.user_get_search_cases_query(user_case_id_tuple,

                                                                                limit,

                                                                                offset)



            # if "datasetId" in search_filters.keys():

            #     get_cases_query = get_cases_query[:-1]

            #     get_cases_query = get_cases_query + " and dataset_id = '{}';".format(search_filters["datasetId"])

            #

            # if "registrationBa" in search_filters.keys():

            #     get_cases_query = get_cases_query[:-1]

            #     get_cases_query = get_cases_query + " and registration_ba = '{}';".format(

            #         search_filters["registrationBa"])

            #

            # if "sort" in search_filters.keys():

            #     get_cases_query = get_cases_query[:-1]

            #     get_cases_query = get_cases_query + " order by '{}' DESC;".format(search_filters["sort"])





            cases_df = pd.read_sql_query(get_cases_query, conn)



            if user_role == constants.admin:

                sla_query = admin_query_object.get_sla_query()

            else:

                sla_query = user_query_object.user_get_sla_query(user_case_id_tuple)



            if "sla" in search_filters.keys():

                sla_query = sla_query[:-1]

                sla_query = sla_query + " where c.sla = {};".format(search_filters["sla"])



            sla_df = pd.read_sql_query(sla_query, conn)



            if user_role == constants.admin:

                total_violations_query = admin_query_object.get_total_violations_of_cases_query()

            else:

                total_violations_query = user_query_object.user_get_total_violations_of_cases_query(user_case_id_tuple)



            violations_df = pd.read_sql_query(total_violations_query, conn)



            if user_role == constants.admin:

                total_performers_query = admin_query_object.get_total_performers_query()

            else:

                total_performers_query = user_query_object.user_get_total_performers_query(user_case_id_tuple)



            performers_df = pd.read_sql_query(total_performers_query, conn)



            df_dict = {"cases": cases_df, "sla": sla_df, "violations": violations_df, "performers": performers_df}

            final_data_to_return = combine_case_details(conn, df_dict)

            count = len(final_data_to_return)



            final_dict = dict()

            final_dict["cases"] = final_data_to_return

            final_dict["offset"] = offset

            final_dict["limit"] = limit

            final_dict["count"] = count



            print(final_dict)

            return final_dict



        except Exception as e:

            print("Exception occurred in Search Cases API:")

            print(e)

            empty_cases_response = deepcopy(constants.empty_cases_response)

            return empty_cases_response



    elif method == 'GET':

        print("ENTERED CASES API GET CALL")

        query_object = queries.SearchQueries()

        conn = aurora_client.get_db_connection(env, env_db)

        # conn = postgre.create_connection("DEV")

        print("DB Connection made successfully!")

        case_id = pathparams["caseId"]

        get_case_details_query = query_object.get_case_details_by_caseId(case_id)

        case_details_record = pd.read_sql_query(get_case_details_query, conn)

        if case_details_record.empty:

            empty_case_record = deepcopy(constants.cases_dict)

            return empty_case_record

        try:

            violation_id = case_details_record["violation_id"][0]

        except Exception as e:

            print(e)

            violation_id = ''



        case_status_query = query_object.get_case_status_by_violationId(violation_id)

        case_status_record = pd.read_sql_query(case_status_query, conn)

        case_details_record_updated = pd.merge(case_details_record, case_status_record, on='violation_id', how='left')



        case_details_record_updated["priority"] = case_details_record_updated.apply(

            lambda row: utils.calculate_priority_logic(row), axis=1)

        case_details_record_updated = case_details_record_updated.drop(

            ["violation_id", 'case_created', 'ent_confidence_score'], axis=1)

        case_details_record_updated.columns = map(humps.camelize, case_details_record_updated.columns)



        case_details_dict = deepcopy(constants.cases_dict)

        try:

            case_details_dict["caseId"] = str(case_details_record_updated["caseId"].values[0])

        except Exception as e:

            print(e)

            case_details_dict["caseId"] = ""

        try:

            case_details_dict["caseStatus"] = str(case_details_record_updated["caseStatus"].values[0])

        except Exception as e:

            print(e)

            case_details_dict["caseStatus"] = ""

        try:

            case_details_dict["priority"] = str(case_details_record_updated["priority"].values[0])

        except Exception as e:

            print(e)

            case_details_dict["priority"] = ""

        try:

            case_details_dict["datasetId"] = str(case_details_record_updated["datasetId"].values[0])

        except Exception as e:

            print(e)

            case_details_dict["datasetId"] = ""

        try:

            case_details_dict["registrationBa"] = str(case_details_record_updated["registrationBa"].values[0])

        except Exception as e:

            print(e)

            case_details_dict["registrationBa"] = ""



        print(case_details_dict)

        return case_details_dict





def get_all_violations(env, env_db, method, header, request_body={}, pathparams={}):

    if method == 'GET':

        print("Entered Violations API GET Method")

        conn = aurora_client.get_db_connection(env, env_db)

        # conn = postgre.create_connection("PROD")

        print("DB Connection made successfully!")



        case_id = pathparams["caseId"]

        violation_id = pathparams["Id"]

        query_object = queries.SearchQueries()

        get_violation_query = query_object.get_all_violations_by_id(case_id, violation_id)

        get_violation_record = pd.read_sql_query(get_violation_query, conn)

        get_violation_record = get_violation_record.drop(["dataset_id", "registration_ba", "case_id", "sensitive_type"],

                                                         axis=1)

        get_violation_record.columns = map(humps.camelize, get_violation_record.columns)

        try:

            list_of_findings = get_violation_record["findings"].values[0].split('\n')

        except Exception as e:

            print(e)

            list_of_findings = []

        findings_list_to_return = []

        for item in list_of_findings:

            findings_dict = deepcopy(constants.findings_dict)

            if "SENSITIVE_CONTEXT:" in item:

                sensitive_context = utils.remove_prefix(item, "SENSITIVE_CONTEXT: ")

                findings_dict["sensitiveContext"] = sensitive_context

            if "SENSITIVE_VALUE:" in item:

                sensitive_value = utils.remove_prefix(item, "SENSITIVE_VALUE: ")

                findings_dict["sensitiveValue"] = sensitive_value

        findings_list_to_return.append(findings_dict)



        violations_dict = deepcopy(constants.violations_dict)

        try:

            violations_dict["violationId"] = str(get_violation_record["violationId"].values[0])

        except Exception as e:

            print(e)

            violations_dict["violationId"] = ""

        try:

            violations_dict["bucket"] = str(get_violation_record["bucket"].values[0])

        except Exception as e:

            print(e)

            violations_dict["bucket"] = ""

        try:

            violations_dict["objectKey"] = str(get_violation_record["objectKey"].values[0])

        except Exception as e:

            print(e)

            violations_dict["objectKey"] = ""

        try:

            violations_dict["confidenceScore"] = int(get_violation_record["confidenceScore"].values[0])

        except Exception as e:

            print(e)

            violations_dict["confidenceScore"] = ""



        violations_dict["findings"] = findings_list_to_return



        print(violations_dict)

        return violations_dict



    else:

        print("Entered Violations API POST Method")

        engine = aurora_client.get_db_connection(env, env_db)

        query_object = queries.SearchQueries()

        user_query_object = queries.NonAdminSearchQueries()

        # engine = postgre.create_connection("PROD")

        print("DB Connection made successfully!")

        print("Entered Filtration code")

        caseId = pathparams["caseId"]

        print("The case ID is:")

        print(caseId)

        print("Getting User role:")

        user_role = utils.get_user_role(header)

        # user_role = constants.admin

        print(user_role)



        if user_role != constants.admin:

            if user_role == constants.delegate:

                user_id = utils.get_user_eid(header)

                user_case_id_tuple = get_non_admin_cases(user_id, engine)



            else:

                print("Getting User EID:")

                user_id = utils.get_user_eid(header)

                print(user_id)



                user_cases_query = user_query_object.get_user_specific_cases(user_id)

                user_cases_df = pd.read_sql_query(user_cases_query, engine)

                user_case_id_tuple = utils.convert_list_to_sql_query_tuple(list(user_cases_df["case_id"]))



        else:

            print("Entered condition where particular case is required")

            caseId_list = [str(caseId)]

            user_case_id_tuple = utils.convert_list_to_sql_query_tuple(caseId_list)



        body = request_body

        offset = 0

        limit = 30



        if "offset" in request_body.keys():

            offset = request_body["offset"]



        if "limit" in request_body.keys():

            limit = request_body["limit"]



        minConfidenceScore = 0

        maxConfidenceScore = 100

        filters = []

        print("Started Querying:")

        query = query_object.get_violations_query()

        count_query = query_object.get_violations_count_filter_query()



        if 'minConfidenceScore' in body and 'maxConfidenceScore' in body:

            minConfidenceScore = body['minConfidenceScore']

            maxConfidenceScore = body['maxConfidenceScore']



        scorefilter = """  ent_confidence_score::int >= '{0}' and ent_confidence_score::int <= '{1}' """.format(

            minConfidenceScore,

            maxConfidenceScore)

        query += scorefilter

        count_query += scorefilter



        if 'fromDate' in body:

            fromDate = body['fromDate']

            query += " and  case_created::date >= '{0}' ".format(fromDate)

            count_query += " and  case_created::date >= '{0}' ".format(fromDate)



        if 'toDate' in body:

            toDate = body['toDate']

            query += " and  case_created::date <= '{0}' ".format(toDate)

            count_query += " and  case_created::date <= '{0}' ".format(toDate)



        # Non Admin and Admin all cases

        if caseId == '-1' or caseId == -1:

            # Admin all cases

            if user_role == constants.admin:

                query = query

                count_query = count_query

            # Non Admin all cases

            else:

                query = query + "and case_id in {0} ".format(

                    user_case_id_tuple)

                count_query = count_query + "and case_id in {0} ".format(

                    user_case_id_tuple)



        # Non Admin and Admin single case scenario  (any user, irrespective of role)

        else:

            query = query + "and case_id in {0} ".format(

                user_case_id_tuple)

            count_query = count_query + "and case_id in {0} ".format(

                user_case_id_tuple)



        if 'datasetId' in body and body['datasetId'] is not None and body['datasetId'] != "":

            datasetId = body['datasetId']

            query = query + " and dataset_id =" + "'" + str(datasetId) + "'"

            count_query = count_query + " and dataset_id =" + "'" + str(datasetId) + "'"



        if 'caseId' in body and body['caseId'] is not None and body['caseId'] != "":

            cId = body['caseId']

            query = query + " and case_id =" + "'" + str(cId) + "'"

            count_query = count_query + " and case_id =" + "'" + str(cId) + "'"



        if 'sensitiveType' in body and body['sensitiveType'] is not None and body['sensitiveType'] != "":

            sensitiveType = body['sensitiveType']

            query = query + " and sensitive_type  ILIKE " + "'" + "%%" + str(sensitiveType) + "%%" + "'"

            count_query = count_query + " and sensitive_type  ILIKE " + "'" + "%%" + str(sensitiveType) + "%%" + "'"

        if 'sensitiveContext' in body and body['sensitiveContext'] is not None and body['sensitiveContext'] != "":

            sensitiveContext = body['sensitiveContext']

            query = query + " and sample_findings ILIKE " + "'" + "%%" + str(sensitiveContext) + "%%" + "'"

            count_query = count_query + " and  sample_findings ILIKE " + "'" + "%%" + str(sensitiveContext) + "%%" + "'"

        query = query + "ORDER BY ent_confidence_score DESC OFFSET {0} LIMIT {1} ".format(offset, limit)

        print(query)

        results = engine.execute(query).fetchall()



        total_violations_count = 0

        print("Entered condition where all cases are required for ADMIN")

        try:

            print(count_query)

            total_violations_count = \

                pd.read_sql_query(count_query, engine)["totalviolationscount"].values[0]

        except Exception as e:

            print("Exception occurred while fetching Total Violations count")

            print(e)

            print(count_query)

            total_violations_count = 0



        violations = []

        for row in results:

            try:

                violation = dict(row)

            except Exception as e:

                print(e)

                break

            for key in violation.copy():

                newKey = humps.camelize(key)

                violation[newKey] = violation.pop(key)

                if 'sample_findings' in violation and violation['sample_findings'] is not None:

                    ls = violation['sample_findings'].split(";")

                    vals = []

                    for i in ls:

                        k = i.strip('\n')

                        j = k.split('\n', 2)

                        final = {}

                        for val in j:

                            try:

                                key = val.split(':', 1)[0]

                                value = val.split(':', 1)[1]

                                final[humps.camelize(key.lower())] = value

                            except Exception as e:

                                final[humps.camelize(key.lower())] = val

                                pass

                                print("Exception occured while splitting", e)

                        vals.append(final)

                    violation['temp_findings'] = vals

                    violation['bucketData'] = violation['temp_findings']

                    violation.pop('temp_findings')

            violations.append(violation)



        final_results = []

        for violation in violations:

            violations_dict = {"caseId": "", "violationId": "", "datasetId": "", "bucket": "", "objectKey": "",

                               "sensitiveType": "",

                               "confidenceScore": 0, "findings": []}

            violations_dict["violationId"] = violation["violationId"]

            violations_dict["caseId"] = violation["caseId"]

            violations_dict["datasetId"] = violation["datasetId"]

            violations_dict["bucket"] = violation["bucket"]

            violations_dict["objectKey"] = violation["objectKey"]

            violations_dict["sensitiveType"] = violation["sensitiveType"]

            violations_dict["confidenceScore"] = violation["entConfidenceScore"]



            sample_finding = violation["sampleFindings"]

            sample_findings_list = sample_finding.split('\nSCAN_FINDING_SAMPLE:::\n')

            findings_per_case_list = []

            for finding in sample_findings_list:

                findings_list = (finding.split('\n'))

                findings_dict = {"sensitiveContext": "", "sensitiveValue": ""}

                for item in findings_list:

                    if "SENSITIVE_CONTEXT:" in item:

                        sensitive_context = utils.remove_prefix(item, "SENSITIVE_CONTEXT: ")

                        findings_dict["sensitiveContext"] = sensitive_context

                    if "SENSITIVE_VALUE:" in item:

                        sensitive_value = utils.remove_prefix(item, "SENSITIVE_VALUE: ")

                        findings_dict["sensitiveValue"] = sensitive_value



            findings_per_case_list.append(findings_dict)



            violations_dict["findings"] = findings_per_case_list

            final_results.append(violations_dict)



        final_response_to_return = dict()

        final_response_to_return["violations"] = final_results

        final_response_to_return["offset"] = offset

        final_response_to_return["limit"] = limit

        final_response_to_return["count"] = int(total_violations_count)



        print(final_response_to_return)

        return final_response_to_return





def performers_api(env, env_db, method, header, request_body={}, pathparams={}):

    if method == 'GET':

        print("Entered Performers API GET Method")

        case_id = pathparams["caseId"]

        conn = aurora_client.get_db_connection(env, env_db)

        # conn = postgre.create_connection("QA")

        print("DB Connection made successfully!")

        query_object = queries.SearchQueries()

        get_performers_query = query_object.get_all_performers_from_case_user_table(case_id)

        try:

            performers_record = pd.read_sql_query(get_performers_query, conn)

            print(performers_record)

        except Exception as e:

            print(e)

        performers_record.columns = map(humps.camelize, performers_record.columns)

        final_response = performers_record.to_json(orient='records')



        return final_response



    if method == 'POST':

        print("Entered Performers API Update method:")

        query_object = queries.SearchQueries()

        conn = aurora_client.get_db_connection(env, env_db)

        # conn = postgre.create_connection("QA")

        print("DB Connection made successfully!")

        try:

            added_by = utils.get_user_eid(header)

        except Exception as e:

            print(e)

            added_by = 'lambda'



        print('Added by value:')

        print(added_by)

        case_id = pathparams["caseId"]

        print('caseId is:')

        print(case_id)

        try:

            get_catalog_dataset_id_query = query_object.get_dataset_catalog_id(case_id)

            catalog_dataset_id_df = pd.read_sql_query(get_catalog_dataset_id_query, conn)

            catalog_dataset_id = str(catalog_dataset_id_df["catalog_dataset_id"].values[0])

        except Exception as e:

            print("Exception occurred while retrieving catalog dataset ID")

            print(e)

            catalog_dataset_id = ''



        print("catalog_dataset_id is:")

        print(catalog_dataset_id)



        for item in request_body["performers"]:

            try:

                update_performers_query = query_object.update_performers(case_id, item, added_by, catalog_dataset_id)

                conn.execute(update_performers_query)

                print("A new performer has been added!!")

            except Exception as e:

                print("Exception while updating performers in Case User table:")

                print(e)



        response = performers_api(env, env_db, "GET", header=header, pathparams=pathparams)



        print(response)

        return response



    if method == 'DELETE':

        print("Entered Performers API DELETE Method")

        case_id = pathparams["caseId"]

        performer_id = pathparams["Id"]

        print(case_id)

        print(performer_id)

        conn = aurora_client.get_db_connection(env, env_db)

        # conn = postgre.create_connection("QA")

        print("DB Connection made successfully!")

        query_object = queries.SearchQueries()

        delete_performers_query = query_object.delete_performers_query(case_id, performer_id)

        print(delete_performers_query)

        rows = conn.execute(delete_performers_query)

        if rows.rowcount == 1:

            print("Record Deleted")

            return "Deleted"

        elif rows.rowcount == 0:

            print("Record Not Deleted")

            return "Not Deleted"





def summarize_api(env, env_db, header):

    try:

        print("Entered Summary API")

        conn = aurora_client.get_db_connection(env, env_db)

        # conn = postgre.create_connection("PROD")

        print("DB Connection made successfully!")

        admin_query_object = queries.SearchQueries()

        user_query_object = queries.NonAdminSearchQueries()

        print("Getting User Role:")

        user_role = utils.get_user_role(header)

        # user_role = constants.admin

        print(user_role)



        if user_role == constants.admin:

            get_case_records_query = admin_query_object.get_all_case_records_summary_api()

        else:

            if user_role == constants.delegate:

                user_id = utils.get_user_eid(header)

                case_id_tuple = get_non_admin_cases(user_id, conn)

                get_case_records_query = user_query_object.user_get_all_case_records_summary_api(case_id_tuple)

            else:

                print("Getting User EID:")

                user_id = utils.get_user_eid(header)

                # user_id = 'ABC100'

                print(user_id)

                list_of_case_id_query = user_query_object.get_user_specific_cases(user_id)

                case_id_list = list((pd.read_sql_query(list_of_case_id_query, conn)["case_id"]))

                case_id_tuple = utils.convert_list_to_sql_query_tuple(case_id_list)

                get_case_records_query = user_query_object.user_get_all_case_records_summary_api(case_id_tuple)



        case_record = pd.read_sql_query(get_case_records_query, conn)



        if case_record.empty:

            empty_summary_dict = deepcopy(constants.summary_dict)

            return empty_summary_dict



        if user_role == constants.admin:

            get_total_violations_query = admin_query_object.get_total_violations_of_cases_query()

        else:

            get_total_violations_query = user_query_object.user_get_total_violations_of_cases_query(case_id_tuple)



        violations_record = pd.read_sql_query(get_total_violations_query, conn)



        case_record = pd.merge(case_record, violations_record, on='case_id', how='left')



        violations_tuple = utils.convert_list_to_sql_query_tuple(list(case_record["violation_id"]))



        get_case_status_query = admin_query_object.get_case_status_query(violations_tuple)

        case_status_df = pd.read_sql_query(get_case_status_query, conn)



        cases = pd.merge(case_record, case_status_df, on='violation_id', how='left')

        cases["priority"] = cases.apply(lambda row: utils.calculate_priority_logic(row), axis=1)



        highPriorityCases = utils.calculate_high_priority_cases(cases)

        totalCases = len(cases.index)

        totalViolations = sum(cases["totalviolations"])



        newCases, new_cases_list = utils.calculate_new_cases(cases)

        openCases, open_cases_list = utils.calculate_open_case(cases)



        new_cases_df = cases[cases['case_id'].isin(new_cases_list)]

        newViolations = sum(new_cases_df["totalviolations"])



        open_cases_df = cases[cases['case_id'].isin(open_cases_list)]

        openViolations = sum(open_cases_df["totalviolations"])



        if user_role == constants.admin:

            get_performers_count_query = admin_query_object.get_count_of_total_performers_summary_api()

        else:

            get_performers_count_query = user_query_object.user_get_count_of_total_performers_summary_api(case_id_tuple)



        totalPerformers_df = pd.read_sql_query(get_performers_count_query, conn)



        try:

            totalPerformers = totalPerformers_df['totalperformers'].values[0]

        except Exception as e:

            print(e)

            totalPerformers = 0



        summary_response = deepcopy(constants.summary_dict)

        summary_response["highPriorityCases"] = highPriorityCases

        summary_response["totalCases"] = totalCases

        summary_response["newCases"] = newCases

        summary_response["openCases"] = openCases

        summary_response["totalViolations"] = totalViolations

        summary_response["newViolations"] = newViolations

        summary_response["openViolations"] = openViolations

        summary_response["totalPerformers"] = int(totalPerformers)



        print(summary_response)

        return summary_response



    except Exception as e:

        print("Exception:")

        print(e)

        empty_summary_dict = deepcopy(constants.summary_dict)

        return empty_summary_dict





def get_all_pds_delegates(env, env_db, pathparams):

    try:

        print("Entered Get Delegates API")

        conn = aurora_client.get_db_connection(env, env_db)

        # conn = postgre.create_connection("DEV")

        print("DB Connection made successfully!")

        path_parameters = {"pds_eid" if k == 'caseId' else k: v for k, v in pathparams.items()}

        pds_eid = path_parameters["pds_eid"]



        admin_query_object = queries.SearchQueries()

        get_pds_delegates_query = admin_query_object.get_pds_delegates(pds_eid)

        pds_delegates = pd.read_sql_query(get_pds_delegates_query, conn)

        get_delegates_response = deepcopy(constants.get_pds_delegates_response)

        get_delegates_response["pdsDelegates"] = list(pds_delegates["pds_delegate_eid"])

        get_delegates_response["pds"] = [pds_eid]



        print(get_delegates_response)

        return get_delegates_response



    except Exception as e:

        print("Exception:")

        print(e)

        empty_delegates_dict = deepcopy(constants.get_pds_delegates_response)

        return empty_delegates_dict





def update_pds_delegates(env, env_db, method, pathparam, request_body):

    try:

        conn = aurora_client.get_db_connection(env, env_db)

        # conn = postgre.create_connection("DEV")

        admin_query_object = queries.SearchQueries()

        path_parameters = {"pds_eid" if k == 'caseId' else k: v for k, v in pathparam.items()}

        pds_eid = path_parameters["pds_eid"]



        if method == 'POST':

            print("Entered Add PDS Delegates API")

            if "addDelegates" in request_body.keys():

                for delegate_eid in request_body["addDelegates"]:

                    try:

                        add_pds_delegate_query = admin_query_object.add_pds_delegate(pds_eid, delegate_eid)

                        delegates_add_results = conn.execute(add_pds_delegate_query)

                        if delegates_add_results.rowcount != 0:

                            print("A new PDS delegate has been added!!")

                        else:

                            print("Failed to add a new PDS Delegate")

                    except Exception as e:

                        print("Exception while updating PDS delegates")

                        print(e)



            return get_all_pds_delegates(env, env_db, pathparam)



        elif method == 'DELETE':

            print("Entered Delete PDS Delegates API")

            try:

                eid_delegate = pathparam["Id"]

                remove_pds_delegate_query = admin_query_object.remove_pds_delegate(pds_eid, eid_delegate)

                delegates_remove_results = conn.execute(remove_pds_delegate_query)

                if delegates_remove_results.rowcount != 0:

                    print("The PDS Delegate has been removed!")

                    return "Deleted"

                else:

                    print("Failed to remove the PDS Delegate!")

                    return "Failed to delete!"

            except Exception as e:

                print("Exception while updating PDS delegates")

                print(e)



    except Exception as e:

        print("Exception:")

        print(e)

        empty_delegates_dict = deepcopy(constants.get_pds_delegates_response)

        return empty_delegates_dict





def case_update_status_api(env, env_db, header, request_body, pathparam):

    try:

        print("Entered Case Status Update API")

        conn = aurora_client.get_db_connection(env, env_db)

        # conn = postgre.create_connection("DEV")

        print("DB Connection made successfully!")

        admin_query_object = queries.SearchQueries()

        uuid_to_update = request_body["requestUuid"]

        updated_by_eid = utils.get_user_eid(header)

        # updated_by_eid = "CAD901"



        if request_body["responseStatus"] == "success":

            case_id_to_update = pathparam["caseId"]

            violation_status_to_update = request_body["violationStatus"]

            planned_remediation_date_to_update = request_body["plannedRemediationDate"]

            utep_number_to_update = request_body["exceptionNumber"]

            utep_exception_reason_to_update = request_body["reasonForNoException"]

            case_update_dict = dict()

            case_update_dict["case_id"] = case_id_to_update

            case_update_dict["violation_status"] = violation_status_to_update

            case_update_dict["utep_number"] = utep_number_to_update

            case_update_dict["exception_reason"] = utep_exception_reason_to_update

            case_update_dict["planned_remediation_date"] = planned_remediation_date_to_update

            case_update_dict["updated_by_eid"] = updated_by_eid

            case_update_status_query = admin_query_object.get_insert_case_update_query(case_update_dict)

            print(case_update_status_query)

            case_update_results = conn.execute(case_update_status_query)

            # if case_update_results.rowcount != 0:



        # Inserting into Audit Table

        audit_log_request_body = deepcopy(request_body)

        del audit_log_request_body["responseStatus"]

        del audit_log_request_body["requestUuid"]

        audit_log_request_body["caseId"] = pathparam["caseId"]

        audit_log_dict = dict()

        audit_log_dict["request_id"] = uuid_to_update

        audit_log_dict["initiated_eid"] = updated_by_eid

        audit_log_dict["action"] = constants.case_updated

        audit_log_dict["action_details"] = json.dumps(audit_log_request_body)



        if request_body["responseStatus"] == "success":

            audit_log_dict["action_status"] = constants.case_success

        else:

            audit_log_dict["action_status"] = constants.case_failure



        audit_log_insert_query = admin_query_object.get_insert_audit_log_query(audit_log_dict)

        conn.execute(audit_log_insert_query)



        return get_case_details(conn, request_body, [pathparam["caseId"]])



    except Exception as e:

        print("Exception occurred in Case Status Update API:")

        print(e)

        empty_cases_response = deepcopy(constants.empty_cases_response)

        return empty_cases_response





def get_non_admin_cases(user_id, conn):

    admin_query_object = queries.SearchQueries()

    user_query_object = queries.NonAdminSearchQueries()

    get_all_tagged_pds_eid_query = admin_query_object.get_all_pds_eid_for_delegate(user_id)

    all_pds_eids_for_delegate_df = pd.read_sql_query(get_all_tagged_pds_eid_query, conn)

    all_pds_eid_tuple = utils.convert_list_to_sql_query_tuple(list(all_pds_eids_for_delegate_df["pds_eid"]))

    delegate_cases_df = pd.read_sql_query(user_query_object.get_all_cases_pds_delegate(all_pds_eid_tuple), conn)

    user_cases_tuple = utils.convert_list_to_sql_query_tuple(list(delegate_cases_df["case_id"]))

    return user_cases_tuple





def get_case_details(conn, request_body, case_id_list):

    print("reached get case details function")

    user_query_object = queries.NonAdminSearchQueries()

    search_filters = dict(request_body)



    if "limit" in search_filters.keys():

        limit = search_filters["limit"]

    else:

        limit = 1500



    if "offset" in search_filters.keys():

        offset = search_filters["offset"]

    else:

        offset = 0



    user_case_id_tuple = utils.convert_list_to_sql_query_tuple(case_id_list)



    get_cases_query = user_query_object.user_get_search_cases_query(user_case_id_tuple,

                                                                    limit,

                                                                    offset)

    cases_df = pd.read_sql_query(get_cases_query, conn)



    if cases_df.empty:

        empty_case_record = deepcopy(constants.cases_dict)

        return empty_case_record



    sla_query = user_query_object.user_get_sla_query(user_case_id_tuple)

    sla_df = pd.read_sql_query(sla_query, conn)



    total_violations_query = user_query_object.user_get_total_violations_of_cases_query(user_case_id_tuple)

    violations_df = pd.read_sql_query(total_violations_query, conn)



    total_performers_query = user_query_object.user_get_total_performers_query(user_case_id_tuple)

    performers_df = pd.read_sql_query(total_performers_query, conn)



    df_dict = {"cases": cases_df, "sla": sla_df, "violations": violations_df, "performers": performers_df}

    final_data_to_return = combine_case_details(conn, df_dict)



    count = len(final_data_to_return)



    final_dict = dict()

    final_dict["cases"] = final_data_to_return

    final_dict["offset"] = offset

    final_dict["limit"] = limit

    final_dict["count"] = count



    print(final_dict)

    return final_dict





def combine_case_details(conn, dataframes_dict):

    admin_query_object = queries.SearchQueries()



    cases_df = dataframes_dict["cases"]

    sla_df = dataframes_dict["sla"]

    violations_df = dataframes_dict["violations"]

    performers_df = dataframes_dict["performers"]



    cases_combined_df = pd.merge(cases_df, sla_df, on='case_id', how='left')

    # cases_combined_df = pd.merge(cases_df, sla_df, on=['case_id', 'case_created'], how='left')

    cases_combined_df = cases_combined_df.merge(violations_df, how='left', on='case_id')

    cases_combined_df = cases_combined_df.merge(performers_df, how='left', on='case_id')



    violations_list = utils.convert_list_to_sql_query_tuple(list(cases_df["violation_id"]))



    case_status_query = admin_query_object.get_case_status_query(violations_list)

    case_status_df = pd.read_sql_query(case_status_query, conn)



    cases_combined_df = cases_combined_df.merge(case_status_df, how='left', on='violation_id')

    cases_combined_df["priority"] = cases_combined_df.apply(lambda row: utils.calculate_priority_logic(row),

                                                            axis=1)



    cases_combined_df["sla_y"] = cases_combined_df["sla_y"].apply(

        lambda row: utils.convert_datetime_to_days(row))

    cases_combined_df['sla'] = np.where(cases_combined_df['sla_y'].isna(), cases_combined_df['sla_x'],

                                        cases_combined_df['sla_y'])



    cases_combined_df.columns = map(humps.camelize, cases_combined_df.columns)



    user_all_cases_tuple = utils.convert_list_to_sql_query_tuple(list(cases_combined_df["caseId"]))

    cases_update_status_query = admin_query_object.get_case_update_status(user_all_cases_tuple)

    cases_update_status_df = pd.read_sql_query(cases_update_status_query, conn)

    cases_update_status_df.columns = map(humps.camelize, cases_update_status_df.columns)



    cases_combined_df = cases_combined_df.merge(cases_update_status_df, on='caseId', how='left')

    cases_combined_df['isCaseStatusUpdated'] = np.where(cases_combined_df.caseId.isin(cases_update_status_df.caseId),

                                                        True, False)



    final_data_to_return = cases_combined_df.drop(["violationId", "entConfidenceScore", "slaX", "slaY", "caseCreatedDate"],

                                                  axis=1)



    final_data_to_return["caseCreated"] = final_data_to_return['caseCreated'].astype(str)

    final_data_to_return["dueDate"] = final_data_to_return['dueDate'].astype(str)

    final_data_to_return["plannedRemediationDate"] = final_data_to_return['plannedRemediationDate'].astype(str)



    final_data_to_return["caseCreated"] = final_data_to_return["caseCreated"].replace('NaT', '')

    final_data_to_return["dueDate"] = final_data_to_return["dueDate"].replace('NaT', '')

    final_data_to_return["plannedRemediationDate"] = final_data_to_return["plannedRemediationDate"].replace('NaT', '')

    final_data_to_return["plannedRemediationDate"] = final_data_to_return["plannedRemediationDate"].replace('nan', '')



    final_data_to_return['caseStatus'] = final_data_to_return['caseStatus'].fillna("OPEN")



    final_data_to_return.rename(

        {'totalviolations': 'totalViolations', 'caseCreated': 'createdDate',

         'totalperformers': 'totalPerformers',

         'slaDays': 'sla'},

        axis='columns', inplace=True)



    final_data_to_return = final_data_to_return.fillna('')

    final_data_to_return = final_data_to_return.to_dict('records')



    return final_data_to_return