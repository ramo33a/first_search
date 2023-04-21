summary_dict = {

    "highPriorityCases": 0,

    "totalCases": 0,

    "newCases": 0,

    "openCases": 0,

    "totalViolations": 0,

    "newViolations": 0,

    "openViolations": 0,

    "totalPerformers": 0

}



lambda_response = {

    "statusCode": 200,

    "statusDescription": "200 OK",

    "headers": {

        "Content-Type": "application/json"

    },

    "body": ""

}



findings_dict = {

    "sensitiveContext": "",

    "sensitiveValue": ""

}



violations_dict = {

    "violationId": "",

    "bucket": "",

    "objectKey": "",

    "confidenceScore": 0,

    "findings": []

}



cases_dict = {

    "caseId": "",

    "caseStatus": "",

    "priority": "",

    "datasetId": "",

    "registrationBa": ""

}



get_pds_delegates_response = {

    "pdsDelegates": [],

    "performers": [],

    "pds": []

}



violation_status_mapper = {'False positive': 'FALSE_POSITIVE', 'Rescan (true positive)': 'RESCANNING_TRUE_POSITIVE',

                           'Acknowledged': 'ACKNOWLEDGED', 'Rescan (unevaluated or mixed)': 'RESCANNING_NOT_VERIFIED',

                           'Planned Remediation': 'PLANNED_REMEDIATION'}



empty_cases_response = {"cases": [], "count": 0, "limit": 0, "offset": 0}





# Roles

admin = 'admin'

pds = 'pds'

user = 'user'

delegate = 'delegate'



# Actions

case_updated = "case_updated"

case_success = "success"

case_failure = "failure"



# Methods

post = "POST"

get = "GET"

