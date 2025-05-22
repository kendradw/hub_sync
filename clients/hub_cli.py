#region ---- Imports ----
from dataclasses import asdict
import json
import hubspot
from pprint import pprint
from hubspot.crm.contacts import (
    SimplePublicObjectInput, #batch create
    BatchInputSimplePublicObjectBatchInputForCreate, #batch create
    SimplePublicUpsertObject, #batch update
    BatchInputSimplePublicObjectBatchInputUpsert, #batch update
    BatchInputSimplePublicObjectId, #batch delete
    ApiException, 
    )
from datetime import datetime
# Local imports
import configs.crypter as crypter
from configs.setup_logger import setup_logger
from configs.dataclasses import Employee
#endregion

class HubspotClient():
    def __init__(self):
        #Logger
        self.log = setup_logger(__name__)
        # Const variables
        config = self.load_config()
        self.HB_DB_COMPANY_ID = config.get("HB_DB_COMPANY_ID")
        # tokens / client
        self.hb_token = crypter.decrypt_from_config("hubspot_token")
        self.hub = hubspot.Client.create(access_token=self.hb_token)

    def contact_search(self, search_filters:dict):
        """Searches Hubspot contacts basaed on search_filters.
        Parameters:
            search_filters: hubspot CRM API search filters: https://developers.hubspot.com/docs/guides/api/crm/search 
        Returns:
            List of Hubspot result objects"""
        try:
            results = []
            after = None
            while True:
                if after: #set paging
                    search_filters["after"] = after
                response = self.hub.crm.contacts.search_api.do_search(
                    public_object_search_request=search_filters
                )
                results.extend(response.results)
                if response.paging and response.paging.next:
                    after = response.paging.next.after
                else:
                    break
            self.log.info(f"Retrieved {len(results)} contacts from search")
            return results
        except ApiException as e:
            self.log.error(f"HubSpot API search error: {e}")

#region ---- Employee Specific ----
    def get_employees(self):
        """Searches for contacts with "Dowbuilt Employee" as marketing classification or @dowbuilt.com email address.
        Returns: list of Employee objects """
        search_request = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "CONTAINS_TOKEN",
                    "value": "@dowbuilt.com"
                }],
                "filters":[{
                    "propertyName":"marketing_classification",
                    "operator": "EQ",
                    "value": "Dowbuilt Employee"
                }]
            }],
            "properties": ["email", "firstname", "lastname", "state", "dowbuilt_region", "marketing_classification", "company"],
            "limit": 100  # Max per page
            }
        results = self.contact_search(search_request)
        contacts = []
        for contact in results:
            contacts.append(contact.to_dict()) #convert from hubspot object to dict
        self.hub_employees = self._convert_employees(contacts)
        return self.hub_employees

    def batch_delete(self, contacts:list[Employee]):
        """Takes a list of contact id's and batch archives/deletes. Batches of 100.
        Returns:
            list of emails of employees that weer archived"""
        archived = [] #confirmed archived list
        for chunk in self.chunk_list(contacts, 100):
            inputs = [{"id": emp.hub_id} for emp in chunk] # Wrap each ID in the required format
            batch_input = BatchInputSimplePublicObjectId(inputs=inputs)
            try:
                response = self.hub.crm.contacts.batch_api.archive(
                    batch_input_simple_public_object_id=batch_input
                )
                archived.extend(chunk)
                self.log.info(f"{len(inputs)} Contacts successfully archived.")
                self.log.debug(inputs)
            except ApiException as e:
                self.log.error(f"Error archiving contacts: {e}")
        return archived
    
    def batch_create_employees(self, employees:list[Employee]):
        """Takes list of employee objects and batch creates in hubspot.
        Returns:
            List of user emails that were created."""
        created = []
        # Batch create contacts
        for chunk in self.chunk_list(employees, 100):
            inputs = [self._create_employee_payload(emp) for emp in chunk]
            bispobifc = BatchInputSimplePublicObjectBatchInputForCreate(inputs=inputs)
            try:
                response = self.hub.crm.contacts.batch_api.create(batch_input_simple_public_object_batch_input_for_create=bispobifc)
                created.extend(chunk)
                self.log.info(f"{len(inputs)} Contacts successfully created at {response.completed_at}.")
                self.log.debug(inputs)
            except ApiException as e:
                self.log.error(f"Exception when calling batch_api->create: {e}")
        return created
    def batch_update(self, employees:list[Employee]):
        updated = [] 
        for chunk in self.chunk_list(employees, 100):
            inputs = [self._create_update_payload(emp) for emp in chunk]
            bispobiu = BatchInputSimplePublicObjectBatchInputUpsert(inputs=inputs)
            try:
                api_response = self.hub.crm.contacts.batch_api.upsert(batch_input_simple_public_object_batch_input_upsert=bispobiu)
                updated.extend(chunk)
                self.log.info(f"{len(inputs)} Contacts successfully updated.")
                self.log.debug(inputs)
                return updated
            except ApiException as e:
                self.log.error(f"Exception when calling batch_api->create: {e}")
        return updated

    #region -- Employee Specific Helper Functions --
    def _convert_employees(self, results:list):
        """Converts from hubspot object to Employee Object
        Params:
            results: list of dictionary items
        Returns:
            list of Employee objects"""
        employees = []
        for employee in results:
            new_employee = Employee(
                hub_id= employee.get("id"),
                first_name = employee.get("properties", {}).get("firstname"),
                last_name = employee.get("properties", {}).get("lastname"),
                email = employee.get("properties", {}).get("email"),
                state = employee.get("properties").get("state"),
                region = employee.get("properties", {}).get("dowbuilt_region"),
                marketing_classification = employee.get("properties", {}).get("marketing_classification"), #should be everyone in this list
                company = employee.get("properties", {}).get("company")
            )
            employees.append(new_employee)
        self.log.info(f"Converted {len(employees)} employee contacts")
        #TODO: delete
        with open("hubspot.json", "w") as of:
            json.dump({emp.email: asdict(emp) for emp in employees}, of, indent=2)
        return employees
    
    def _create_employee_payload(self, employee:Employee) -> SimplePublicObjectInput:
        return SimplePublicObjectInput(
            properties={
                "email": employee.email,
                "firstname": employee.first_name,
                "lastname": employee.last_name,
                "state": employee.state,
                "dowbuilt_region": employee.region,
                "marketing_classification": employee.marketing_classification,
                "company": employee.company,
            }
        )
    
    def _create_update_payload(self, employee:Employee):
        return {
            "id":employee.email,
            "idProperty": "email",
            "properties":{
                "email": employee.email,
                "firstname": employee.first_name,
                "lastname": employee.last_name,
                "state": employee.state,
                "dowbuilt_region": employee.region,
                "marketing_classification": employee.marketing_classification,
                "company": employee.company,
            }
        }
    #endregion
#endregion
    
#region ---- Helper Functions ----

    def chunk_list(self, data, chunk_size):
        """Helper function to yield successive chunks from list."""
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    def load_config(self, file_path = "configs/config.json"):
        """Load the config file"""
        try:
            with open(file_path, "r") as inf:
                return json.load(inf)
        except Exception as e:
            self.log.error(f"ERROR: loading config file {e}")

    def convert_datetime(self, obj):
        """Convert datetime for output to json file.
        (really just so i can look at stuff)
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
#endregion
