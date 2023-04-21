class SearchQueries:

    def __init__(self):

        self.scan_alerts_schema = "scan_alerts"

        self.scan_violation_table = "scan_violations"

        self.scan_utep_table = "scan_utep"

        self.violation_update_status_table = "violation_update_status_bc"

        self.case_user_table = "case_user"

        self.pds_delegate_info_table = "pds_delegate_info"

        self.case_update_status_table = "case_update_status"

        self.audit_log_table = "audit_log"



    def get_search_cases_query(self, limit, offset):

        search_query = "SELECT case_id, dataset_id, platform_type, sensitive_type, element_name, registration_ba,  case_created as case_created_date," \

                       " max(violation_id) as violation_id, max(ent_confidence_score) as ent_confidence_score FROM {0}.{1} " \

                       "group by case_id, platform_type, sensitive_type, element_name, dataset_id, registration_ba, case_created_date " \

                       "order by case_created_date desc limit {2} offset {3} ;".format(self.scan_alerts_schema,

                                                                                       self.scan_violation_table, limit, offset)

        return search_query



    # def get_sla_query(self):

    #     sla_query = "WITH cte AS (select distinct case_id, case_created, case_created + interval '90' day as due_date, " \

    #                 "((case_created+ interval '90' day)::date - current_date ::date) as sla from {0}.{1}) " \

    #                 "SELECT * FROM cte as c;".format(self.scan_alerts_schema, self.scan_violation_table)

    #     return sla_query



    def get_sla_query(self):

        sla_query = "select distinct case_id, min(violation_updated) as case_created," \

                    " min(violation_updated) + interval '90' day as due_date, " \

                    "((min(violation_updated)+ interval '90' day)::date - current_date ::date) as sla " \

                    "from {0}.{1} " \

                    "where violation_status in ( 'PENDING', 'ACKNOWLEDGED',  'PLANNED_REMEDIATION', 'RESCANNING', 'RESCANNING_NOT_VERIFIED', 'RESCANNING_TRUE_POSITIVE') " \

                    "group by case_id;".format(self.scan_alerts_schema, self.violation_update_status_table)



        return sla_query



    def get_case_update_status(self, case_id_tuple):

        case_status_query = "SELECT case_id, violation_status, planned_remediation_date " \

                            "from {0}.{1} where case_id in {2}".format(self.scan_alerts_schema,

                                                                       self.case_update_status_table, case_id_tuple)

        return case_status_query



    def get_insert_case_update_query(self, update_dict):

        if len(update_dict["planned_remediation_date"]) > 0:

            insert_case_update_query = "INSERT INTO {0}.{1} " \

                                       "(case_id, violation_status, utep_number, exception_reason, planned_remediation_date, updated_at, updated_by_eid) " \

                                       "values ('{2}', '{3}', '{4}', '{5}', '{6}', NOW(), '{7}') " \

                                       "ON CONFLICT (case_id) DO UPDATE set violation_status = '{3}', utep_number = '{4}', exception_reason = '{5}', " \

                                       "planned_remediation_date = '{6}', updated_at = NOW(), updated_by_eid = '{7}' " \

                                       "where {0}.{1}.case_id = '{2}'".format(self.scan_alerts_schema,

                                                                              self.case_update_status_table,

                                                                              update_dict["case_id"],

                                                                              update_dict["violation_status"],

                                                                              update_dict["utep_number"],

                                                                              update_dict["exception_reason"],

                                                                              update_dict["planned_remediation_date"],

                                                                              update_dict["updated_by_eid"])

        else:

            insert_case_update_query = "INSERT INTO {0}.{1} " \

                                       "(case_id, violation_status, utep_number, exception_reason, updated_at, updated_by_eid) " \

                                       "values ('{2}', '{3}', '{4}', '{5}', NOW(), '{6}') " \

                                       "ON CONFLICT (case_id) DO UPDATE set violation_status = '{3}', utep_number = '{4}', exception_reason = '{5}', " \

                                       "updated_at = NOW(), updated_by_eid = '{6}' " \

                                       "where {0}.{1}.case_id = '{2}'".format(self.scan_alerts_schema,

                                                                              self.case_update_status_table,

                                                                              update_dict["case_id"],

                                                                              update_dict["violation_status"],

                                                                              update_dict["utep_number"],

                                                                              update_dict["exception_reason"],

                                                                              update_dict["updated_by_eid"])



        return insert_case_update_query



    def get_all_pds_eid_for_delegate(self, delegate_eid):

        get_pds_eid_for_delegate_query = "SELECT DISTINCT pds_eid from {0}.{1} " \

                                         "where pds_delegate_eid = '{2}'".format(self.scan_alerts_schema,

                                                                                 self.pds_delegate_info_table,

                                                                                 delegate_eid)



        return get_pds_eid_for_delegate_query



    def get_insert_audit_log_query(self, audit_log_dict):

        insert_audit_log_query = "INSERT INTO {0}.{1} (request_id, initiated_eid, action, action_details, action_timestamp, action_status) " \

                                 "values ('{2}', '{3}', '{4}', '{5}', NOW(), '{6}')".format(self.scan_alerts_schema,

                                                                                            self.audit_log_table,

                                                                                            audit_log_dict[

                                                                                                "request_id"],

                                                                                            audit_log_dict[

                                                                                                "initiated_eid"],

                                                                                            audit_log_dict["action"],

                                                                                            audit_log_dict[

                                                                                                "action_details"],

                                                                                            audit_log_dict[

                                                                                                "action_status"])

        return insert_audit_log_query



    def get_total_violations_of_cases_query(self, case_id_tuple=None):

        if case_id_tuple is None:

            violations_query = "select case_id, count(case_id) as totalViolations from {0}.{1} group by case_id;".format(

                self.scan_alerts_schema, self.scan_violation_table)

        else:

            violations_query = "select case_id, count(case_id) as totalViolations from {0}.{1} where case_id in {2} group by case_id;".format(

                self.scan_alerts_schema, self.scan_violation_table, case_id_tuple)

        return violations_query



    def get_violations_count_query(self):

        violations_count_query = "select count(*) as totalViolationsCount from {0}.{1};".format(

            self.scan_alerts_schema, self.scan_violation_table)

        return violations_count_query



    def get_total_performers_query(self):

        performers_query = "select case_id, count(*) as totalPerformers from {0}.{1} group by case_id".format(

            self.scan_alerts_schema, self.case_user_table)

        return performers_query



    def get_case_status_query(self, violations_list):

        case_status_query = "select a.violation_id, a.case_status, b.utep_number, b.status, (b.date_expiration-current_date) as sla " \

                            "from {0}.{1} as a left join {0}.{2} as b on " \

                            "a.utep_number=b.utep_number where a.violation_id  in {3}".format(

            self.scan_alerts_schema, self.violation_update_status_table, self.scan_utep_table, violations_list)

        return case_status_query



    def get_all_violations_query(self, violation_filter):

        all_violations_query = "SELECT violation_id, dataset_id, case_id, registration_ba,  bucket, object_key, ent_confidence_score, sensitive_type, sample_findings FROM {0}.{1} where {2} and platform_type = '{3}'".format(

            self.scan_alerts_schema, self.scan_violation_table, violation_filter)

        return all_violations_query



    def get_all_violations_by_id(self, case_id, violation_id):

        violations_query = "SELECT violation_id, dataset_id, case_id, registration_ba,  bucket, object_key, ent_confidence_score as confidence_score, sensitive_type, sample_findings as findings FROM {0}.{1} where case_id = '{2}' and violation_id = '{3}'".format(

            self.scan_alerts_schema, self.scan_violation_table, case_id, violation_id)

        return violations_query



    def get_pds_delegates(self, pds_eid):

        get_pds_delegates_query = "SELECT pds_delegate_eid from {0}.{1} where pds_eid = '{2}';".format(

            self.scan_alerts_schema, self.pds_delegate_info_table, pds_eid)

        return get_pds_delegates_query



    def add_pds_delegate(self, pds_eid, delegate_eid):

        add_pds_delegate_query = "INSERT INTO {0}.{1} (pds_eid, pds_delegate_eid, role_created_at) values ('{2}', '{3}', NOW());".format(

            self.scan_alerts_schema, self.pds_delegate_info_table, pds_eid, delegate_eid)

        return add_pds_delegate_query



    def remove_pds_delegate(self, pds_eid, delegate_eid):

        remove_pds_delegate_query = "DELETE FROM {0}.{1} where pds_eid = '{2}' and pds_delegate_eid = '{3}'".format(

            self.scan_alerts_schema, self.pds_delegate_info_table, pds_eid, delegate_eid)

        return remove_pds_delegate_query



    def get_all_performers_from_case_user_table(self, case_id):

        get_performers_query = "select eid, is_pds from {0}.{1} where case_id = '{2}'".format(

            self.scan_alerts_schema, self.case_user_table, case_id)

        return get_performers_query



    def update_performers(self, case_id, user_id, added_by, catalog_dataset_id):

        update_performers_query = "INSERT INTO {0}.{1} (case_id, eid, added_by_eid, catalog_dataset_id, role) VALUES ('{2}', '{3}', '{4}', '{5}', 'performer')".format(

            self.scan_alerts_schema, self.case_user_table, case_id, user_id, added_by, catalog_dataset_id)

        return update_performers_query



    def get_dataset_catalog_id(self, case_id):

        get_catalog_dataset_id_query = "SELECT catalog_dataset_id from {0}.{1} where case_id = '{2}' limit 1".format(

            self.scan_alerts_schema, self.scan_violation_table, case_id)

        return get_catalog_dataset_id_query



    def delete_performers_query(self, case_id, performerId):

        delete_performers = "delete from {0}.{1} where case_id ='{2}' and eid ='{3}'".format(

            self.scan_alerts_schema, self.case_user_table, case_id, performerId)

        return delete_performers



    def get_case_details_by_caseId(self, case_id):

        get_case_details_query = "WITH cte AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY case_id ) AS rn FROM {0}.{1}) SELECT c.case_id, c.registration_ba, c.violation_id, c.dataset_id, c.case_created, c.ent_confidence_score FROM cte as c WHERE rn = 1 and case_id = '{2}' ".format(

            self.scan_alerts_schema, self.scan_violation_table, case_id)

        return get_case_details_query



    def get_case_status_by_violationId(self, violation_id):

        get_case_status_query = "select a.violation_id, a.case_status, b.utep_number, b.status, (b.date_expiration-current_date) as sla " \

                                "from {0}.{1} as a left join {0}.{2} as b on " \

                                "a.utep_number=b.utep_number where a.violation_id = '{3}'".format(

            self.scan_alerts_schema, self.violation_update_status_table, self.scan_utep_table, violation_id)

        return get_case_status_query



    def get_list_of_case_id_summary_api(self):

        case_id_list_query = "select distinct case_id from {0}.{1}".format(self.scan_alerts_schema,

                                                                           self.scan_violation_table)

        return case_id_list_query



    def get_case_records_summary_api(self, case_id_tuple):

        case_records_query = "SELECT case_id, dataset_id, registration_ba, case_created, max(ent_confidence_score) as ent_confidence_score, max(violation_id) as violation_id, case_created + interval '90' day as due_date, ((case_created+ interval '90' day) - current_date) as sla " \

                             "FROM {0}.{1} WHERE case_id in {2} " \

                             "group by case_id, dataset_id, registration_ba, case_created;".format(

            self.scan_alerts_schema, self.scan_violation_table, case_id_tuple)

        return case_records_query



    def get_all_case_records_summary_api(self):

        case_records_query = "SELECT case_id, dataset_id, registration_ba, case_created, max(ent_confidence_score) as ent_confidence_score, max(violation_id) as violation_id, case_created + interval '90' day as due_date, ((case_created+ interval '90' day) - current_date) as sla " \

                             "FROM {0}.{1} " \

                             "group by case_id, dataset_id, registration_ba, case_created;".format(

            self.scan_alerts_schema, self.scan_violation_table)

        return case_records_query



    def get_count_of_total_performers_summary_api(self):

        count_of_total_performers_query = "select count(distinct eid) as totalPerformers from {0}.{1}".format(

            self.scan_alerts_schema, self.case_user_table)

        return count_of_total_performers_query



    def get_violations_query(self):

        violations_query = "SELECT violation_id, dataset_id, case_id, registration_ba,  bucket, " \

                           "object_key, ent_confidence_score, sensitive_type, sample_findings " \

                           "FROM {0}.{1} as v where ".format(self.scan_alerts_schema, self.scan_violation_table)

        return violations_query



    def get_violations_count_filter_query(self):

        violations_query = "SELECT count(*) as totalviolationscount " \

                           "FROM {0}.{1} as v where ".format(self.scan_alerts_schema, self.scan_violation_table)

        return violations_query





class NonAdminSearchQueries:

    def __init__(self):

        self.scan_alerts_schema = "scan_alerts"

        self.scan_violation_table = "scan_violations"

        self.violation_update_status_table = "violation_update_status_bc"

        self.case_user_table = "case_user"



    def get_user_specific_cases(self, user_id):

        user_cases_query = "select distinct case_id from {0}.{1} where eid= '{2}'".format(self.scan_alerts_schema,

                                                                                          self.case_user_table, user_id)

        return user_cases_query



    def get_all_cases_pds_delegate(self, pds_eid_tuple):

        pds_delegates_cases_query = "select distinct case_id from {0}.{1} where eid in {2}".format(

            self.scan_alerts_schema,

            self.case_user_table, pds_eid_tuple)



        return pds_delegates_cases_query



    def user_get_search_cases_query(self, case_id_tuple, limit, offset):

        user_search_query = "SELECT case_id, dataset_id, platform_type, sensitive_type, element_name, registration_ba,  " \

                            "case_created as case_created_date, max(violation_id) as violation_id, max(ent_confidence_score)" \

                            " as ent_confidence_score FROM {0}.{1} where case_id in {2} group by case_id, platform_type, sensitive_type, element_name, " \

                            "dataset_id, registration_ba, case_created_date order by case_created_date desc limit {3} offset {4}".format(

            self.scan_alerts_schema,

            self.scan_violation_table,

            case_id_tuple, limit, offset)



        return user_search_query



    def user_get_sla_query(self, case_id_tuple):

        user_sla_query = "select distinct case_id, min(violation_updated) as case_created, min(violation_updated) + interval '90' day as due_date, " \

                         "((min(violation_updated)+ interval '90' day)::date - current_date ::date) as sla from {0}.{1} " \

                         "where violation_status in " \

                         "( 'PENDING', 'ACKNOWLEDGED',  'PLANNED_REMEDIATION', 'RESCANNING', 'RESCANNING_NOT_VERIFIED', 'RESCANNING_TRUE_POSITIVE') " \

                         "and case_id in {} group by case_id;".format(self.scan_alerts_schema, self.violation_update_status_table, case_id_tuple)



        return user_sla_query



    def user_get_total_violations_of_cases_query(self, case_id_tuple):

        user_violations_query = "select case_id, count(case_id) as totalViolations from {0}.{1} where case_id in {2} group by case_id;".format(

            self.scan_alerts_schema, self.scan_violation_table, case_id_tuple)



        return user_violations_query



    def user_get_violations_count_query(self, case_id_tuple):

        user_violations_count_query = "select count(*) as totalViolationsCount from {0}.{1} where case_id in {2};".format(

            self.scan_alerts_schema, self.scan_violation_table, case_id_tuple)

        return user_violations_count_query



    def get_violations_count_filter_query(self, case_id_tuple):

        violations_query = "SELECT count(*) as totalViolationsCount " \

                           "FROM {0}.{1} as v where case_id in {2};".format(self.scan_alerts_schema,

                                                                            self.scan_violation_table, case_id_tuple)

        return violations_query



    def user_get_total_performers_query(self, case_id_tuple):

        user_performers_query = "select case_id, count(*) as totalPerformers from {0}.{1} where case_id in {2} group by case_id".format(

            self.scan_alerts_schema, self.case_user_table, case_id_tuple)



        return user_performers_query



    def user_get_all_case_records_summary_api(self, case_id_tuple):

        user_case_records_query = "SELECT case_id, dataset_id, registration_ba, case_created, max(ent_confidence_score) as ent_confidence_score, max(violation_id) as violation_id, case_created + interval '90' day as due_date, ((case_created+ interval '90' day) - current_date) as sla " \

                                  "FROM {0}.{1} WHERE case_id in {2} " \

                                  "group by case_id, dataset_id, registration_ba, case_created;".format(

            self.scan_alerts_schema, self.scan_violation_table, case_id_tuple)



        return user_case_records_query



    def user_get_count_of_total_performers_summary_api(self, case_id_tuple):

        user_count_of_total_performers_query = "select count(distinct eid) as totalPerformers from {0}.{1} where case_id in {2}".format(

            self.scan_alerts_schema, self.case_user_table, case_id_tuple)



        return user_count_of_total_performers_query