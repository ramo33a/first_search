from datetime import datetime, timezone

import jwt

import os

import numpy as np

import json

import math



def calculate_priority_logic(row):

    try:

        date_now = datetime.now(timezone.utc)

        case_created_date = row["case_created"]

        age = (date_now - case_created_date).days



        if math.isnan(row["totalviolations"]):

            volume_alerts = 0

        else:

            volume_alerts = row["totalviolations"]



        is_utep_exists = False

        if row["utep_number"] is not None:

            is_utep_exists = True

            utep_expiration_days = row["sla_y"].days



        if is_utep_exists and utep_expiration_days < 60:

            return "Critical"

        elif is_utep_exists is False and age > 70:

            return "Critical"

        elif 45 < age < 70 and row["ent_confidence_score"] > 0.8:

            return "Very High"

        elif 60 < age < 70 and row["ent_confidence_score"] < 0.8:

            return "Very High"

        elif row["ent_confidence_score"] > 0.8:

            return "High"

        elif 0.5 < row["ent_confidence_score"] < 0.8 and volume_alerts > 1000:

            return "High"

        elif 0.25 < row["ent_confidence_score"] < 0.8:

            return "Medium"

        else:

            return "Low"

    except Exception as e:

        print(e)

        return "Low"





def calculate_high_priority_cases(df):

    priority_count = 0

    for priority, status in zip(df["priority"], df["case_status"]):

        if priority in ("Critical", "Very High", "High") and status == "OPEN":

            priority_count += 1

    return priority_count





def calculate_new_cases(df):

    new_cases_count = 0

    new_cases_list = []

    date_now = datetime.now(timezone.utc)

    for case_id, item in zip(df["case_id"], df["case_created"]):

        age = (date_now - item).days

        if age <= 15:

            new_cases_count = new_cases_count + 1

            new_cases_list.append(case_id)

    return new_cases_count, new_cases_list





def calculate_open_case(df):

    open_cases_count = 0

    open_cases_list = []

    for case_id, item in zip(df["case_id"], df["case_status"]):

        if item == 'OPEN' or item == 'open' or item == 'Open':

            open_cases_count = open_cases_count + 1

            open_cases_list.append(case_id)

    return open_cases_count, open_cases_list





def convert_list_to_sql_query_tuple(item):

    if len(item) == 0:

        item.append('')

        item.append('')

        return tuple(item)

    elif len(item) == 1:

        item.append('')

        return tuple(item)

    else:

        return tuple(item)





def remove_prefix(text, prefix):

    if text.startswith(prefix):

        return text[len(prefix):]

    return text



def get_method_from_event(event):

    try:

        return event["httpMethod"]

    except Exception as e:

        print(e)

        return "GET"





def get_path_from_event(event):

    try:

        return event["path"]

    except Exception as e:

        print(e)

        return "Invalid path"





def get_request_body_from_event(event):

    try:

        if len(event["body"]) == 0:

            return {}



        elif type(event["body"]) == str:

            return json.loads(event["body"])



        else:

            return {}



    except Exception as e:

        print(e)

        return {}





def get_query_params_from_event(event):

    try:

        return event["queryStringParameters"]

    except Exception as e:

        print(e)

        return {}





def get_headers_from_event(event):

    try:

        return event["headers"]

    except Exception as e:

        print(e)

        return {}





def get_path_params_from_event(event):

    try:

        path = event["path"]

        path_list = path.split("/")

        if 5 < len(path_list) < 8:

            pathparams = {'caseId': path_list[5]}

            return pathparams

        elif len(path_list) > 7:

            pathparams = {'caseId': path_list[5], 'Id': path_list[-1]}

            return pathparams

        else:

            return {}

    except Exception as e:

        print(e)

        return {}



def get_user_eid(headers):

    try:

        print("Entered Getting USER EID function:")

        token = headers["pingid-token"]

        user_json = jwt.decode(token, options={"verify_signature": False})

        eid = user_json['workforce_auth_info']['userid']

        print(eid)

        return eid



    except Exception as e:

        print(e)

        return None



def get_user_role(headers):

    try:

        print("Entered Getting User Role Function:")

        token = headers["pingid-token"]

        user_json = jwt.decode(token, options={"verify_signature": False})

        ad_groups = user_json['workforce_auth_info']['ADGroups']

        env = os.getenv("APP_ENV")



        if 'prod' in env.lower():

            environment = 'prod'

        else:

            environment = 'qa'



        ad_groups = list(map(str.lower, ad_groups))

        print("Printing the AD GROUPS:")

        print(ad_groups)



        roles_in_current_env = [role for role in ad_groups if environment in role]

        print("Print the list of environment specific roles:")

        print(roles_in_current_env)



        combined_roles = '\t'.join(roles_in_current_env)

        print("Printing all the roles in the environment:")

        print(combined_roles)



        if "admin" in combined_roles:

            print("USER ROLE IS:")

            print("ADMIN")

            return 'admin'



        elif "pds" in combined_roles and "delegate" not in combined_roles:

            print("USER ROLE IS:")

            print("PDS")

            return 'pds'



        elif "delegate" in combined_roles:

            print("USER ROLE IS:")

            print("PDS DELEGATE")

            return 'delegate'

        else:

            print("USER ROLE IS:")

            print("USER")

            return 'user'



    except Exception as e:

        print(e)

        return 'user'



def convert_datetime_to_days(row):

    try:

        days = row.days

        return days



    except Exception as e:

        print(e)

        return np.nan

