import os

import logging

from lambda_directory import utils

from lambda_directory import constants

import json

from lambda_directory import scan_nav_api

from copy import deepcopy



logger = logging.getLogger()

env = os.getenv("APP_ENV")

db_env = os.getenv("DB_ENV")



region_name = os.getenv("AWS_REGION")



if env and env.lower() == 'prod':

    logger.setLevel(logging.ERROR)

else:

    logger.setLevel(logging.INFO)





def lambda_handler(event, context) -> dict:

    print("Printing Event")

    print(event)

    method = utils.get_method_from_event(event)

    path = utils.get_path_from_event(event)

    request_body = utils.get_request_body_from_event(event)

    queryparams = utils.get_query_params_from_event(event)

    headers = utils.get_headers_from_event(event)

    pathparams = utils.get_path_params_from_event(event)



    # Validate PoP token

    # exchange_gateway.auth.validate(method, path, headers, exchange_host, require_uid_header=False)



    print("Printing Method, Path, Request Body, Query Parameters and Path Parameters for the current Event:")

    print(method, path, request_body, queryparams, pathparams)



    if '/health' in path:

        print("Inside Health API")

        health_lambda_response = deepcopy(constants.lambda_response)

        health_lambda_response["body"] = "Healthy Search Lambda"

        return health_lambda_response



    elif 'performers' in path and 'summarise' not in path:

        try:

            print("Inside Performers API")

            performers_response = scan_nav_api.performers_api(env, db_env, method, headers, request_body, pathparams)

            if method != 'DELETE':

                print("Inside Get Method")

                performers_lambda_response = deepcopy(constants.lambda_response)

                performers_lambda_response["body"] = json.dumps(performers_response)

                if method == 'POST':

                    print("Inside Post Method")

                    performers_lambda_response["statusCode"] = 201

                    performers_lambda_response["statusDescription"] = "201 Created"

                return performers_lambda_response

            else:

                if performers_response == "Deleted":

                    print("Performers deleted")

                    delete_performers_lambda_response = deepcopy(constants.lambda_response)

                    delete_performers_lambda_response["statusCode"] = 204

                    delete_performers_lambda_response["body"] = ""

                    delete_performers_lambda_response["statusDescription"] = "204 Deleted"

                    return delete_performers_lambda_response

                else:

                    print("No deletions!")

                    lambda_response = deepcopy(constants.lambda_response)

                    lambda_response["body"] = ""

                    return lambda_response

        except Exception as e:

            performers_lambda_response_excptn = deepcopy(constants.lambda_response)

            performers_lambda_response_excptn["body"] = str(e)

            return performers_lambda_response_excptn



    elif 'violations' in path:

        try:

            print("Inside Violations API")

            violations_response = scan_nav_api.get_all_violations(env, db_env, method, headers, request_body,

                                                                  pathparams)

            violations_lambda_response = deepcopy(constants.lambda_response)

            violations_lambda_response["body"] = json.dumps(violations_response)

            if method == 'POST':

                violations_lambda_response["statusCode"] = 201

                violations_lambda_response["statusDescription"] = "201 Created"

            return violations_lambda_response

        except Exception as e:

            violations_lambda_response_excptn = deepcopy(constants.lambda_response)

            violations_lambda_response_excptn["body"] = str(e)

            return violations_lambda_response_excptn



    elif '/summarise-cases-perfomers' in path:

        try:

            print("Inside Summary API")

            summary_response = scan_nav_api.summarize_api(env, db_env, headers)

            summary_lambda_response = deepcopy(constants.lambda_response)

            summary_lambda_response["body"] = json.dumps(summary_response)

            return summary_lambda_response



        except Exception as e:

            summary_lambda_response_excptn = deepcopy(constants.lambda_response)

            summary_lambda_response_excptn["body"] = str(e)

            return summary_lambda_response_excptn



    elif 'search-cases' in path or 'cases' in path:

        if method == 'PUT':

            print("Inside Case Status Update API")

            case_status_update_response = scan_nav_api.case_update_status_api(env, db_env, headers, request_body, pathparams)

            case_status_update_lambda_response = deepcopy(constants.lambda_response)

            case_status_update_lambda_response["body"] = json.dumps(case_status_update_response)

            return case_status_update_lambda_response

        else:

            try:

                print("Inside Search Cases API")

                search_response = scan_nav_api.get_all_cases(env, db_env, method, headers, request_body, pathparams)

                search_lambda_response = deepcopy(constants.lambda_response)

                search_lambda_response["body"] = json.dumps(search_response)

                if method == 'POST':

                    search_lambda_response["statusCode"] = 201

                    search_lambda_response["statusDescription"] = "201 Created"

                return search_lambda_response

            except Exception as e:

                search_lambda_response_excptn = deepcopy(constants.lambda_response)

                search_lambda_response_excptn["body"] = str(e)

                return search_lambda_response_excptn



    elif 'delegates' in path:

        if method == 'GET':

            print("Inside Get Delegates API")

            get_delegates_response = scan_nav_api.get_all_pds_delegates(env, db_env, pathparams)

            delegates_lambda_response = deepcopy(constants.lambda_response)

            delegates_lambda_response["body"] = json.dumps(get_delegates_response)

            return delegates_lambda_response



        elif method == 'POST':

            update_delegates_response = scan_nav_api.update_pds_delegates(env, db_env, method, pathparams, request_body)

            update_delegate_lambda_response = constants.lambda_response.copy()

            update_delegate_lambda_response["body"] = json.dumps(update_delegates_response)

            return update_delegate_lambda_response



        elif method == 'DELETE':

            delete_delegates_response = scan_nav_api.update_pds_delegates(env, db_env, method, pathparams, request_body)

            if delete_delegates_response == "Deleted":

                print("Delegate deleted")

                delete_delegates_lambda_response = constants.lambda_response.copy()

                delete_delegates_lambda_response["statusCode"] = 204

                delete_delegates_lambda_response["body"] = ""

                delete_delegates_lambda_response["statusDescription"] = "204 Deleted"

                return delete_delegates_lambda_response

            else:

                print("Delegate Deletion Failed")

                lambda_response = constants.lambda_response.copy()

                lambda_response["body"] = "The provided delegateId could not be found"

                lambda_response["statusCode"] = 404

                lambda_response["statusDescription"] = "404 Not Found"

                return lambda_response



    else:

        print("Lambda received Invalid event")

        print(event)

        invalid_lambda_response = deepcopy(constants.lambda_response)

        invalid_lambda_response["body"] = "Invalid input to Lambda"

        return invalid_lambda_response