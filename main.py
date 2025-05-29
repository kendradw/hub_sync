import json
from dataclasses import dataclass, asdict
from configs.setup_logger import setup_logger
from configs.dataclasses import Employee
from clients.grid import grid
import configs.crypter as crypter
from clients.hub_cli import HubspotClient
import pandas as pd
from datetime import datetime

class HubspotEmployeeSync():
    def __init__(self):
        # Logger
        self.log = setup_logger(__name__)

        #const variables
        config = self.load_config()
        self.BAMBOO_DATA_SS_ID = config.get("bamboo_data_ss_id")
        self.HUBSPOT_SS_ID = config.get("hubspot_ss_id")
        self.regions = config.get("regions")
        self.HB_DB_COMPANY_ID = config.get("HB_DB_COMPANY_ID")

        #Tokens
        self.ss_token = crypter.decrypt_from_config("ss_automation_token")
        grid.token = self.ss_token

        #Clients
        self.hub_client = HubspotClient()


#region ---- Main functions ----
    def sync(self):
        bamboo_map = self.get_bamboo_data()
        hub_map = self.hub_client.get_employees()

        create, update, delete, unchanged = self.compare_employee_lists(hub_map, bamboo_map)
        created = self.hub_client.batch_create_employees(create)
        updated = self.hub_client.batch_update(update)
        deleted = self.hub_client.batch_delete(delete)
        self.post_to_ss(created, updated, deleted, unchanged)
        #TODO: verify they the same with self.verify()
        self.log.info("SYNC COMPLETE")

    def compare_employee_lists(self, hubspot, bamboo):
        #map by email
        bamboo_map, hubspot_map = self._map_employees(bamboo), self._map_employees(hubspot)
        create, update, delete, unchanged = [], [], hubspot_map, []
        for email, bamb_contact in bamboo_map.items():
            hub_contact = hubspot_map.get(email, None)
            if hub_contact:
                self.log.debug(f"{email} exists") #if they exist check for updates
                bamb_contact.hub_id = hub_contact.hub_id #add the hubspot id, 
                if hub_contact != bamb_contact: 
                    update.append(bamb_contact) #add the employee object to update
                else:
                    self.log.debug(f"No updates for: {email}")
                    unchanged.append(bamb_contact)
                del delete[email] # the only people left at the end will be people to remove, theoretically
            elif hub_contact is None: #they don't exist in hubspot, they need to be added
                create.append(bamb_contact)
        delete = list(delete.values()) #turn back into list
        self.log.info(f"Found {len(create)} employees to add: \n{create}\n")
        self.log.info(f"Found {len(update)} employees to update: \n{update}\n")
        self.log.info(f"Found {len(delete)} emplopyees to remove: \n{delete}\n")
        self.log.info(f"Found {len(unchanged)} employees with no changes \n{unchanged}")
        return create, update, delete, unchanged

    def post_to_ss(self, created, updated, deleted, unchanged):
        """Syncs updates to Hubspot Log sheet"""
        sheet = grid(self.HUBSPOT_SS_ID)
        sheet.fetch_content()
        df = sheet.df
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        email_lc_list = df["Email"].str.lower().tolist()

        self.log.info(f"Created: {created} \n Updated: {updated} \n Deleted {deleted}")

        post_new = [self.build_row(emp, "Created", now) for emp in created]
        post_update = [self.build_row(emp, "Updated", now) for emp in updated]
        delete_update = [self.build_row(emp, "Deleted", now, removed=True) for emp in deleted]
        no_change_post = [self.build_row(emp, "Initial Sync", now) for emp in unchanged if emp.email not in email_lc_list]
        all_rows = post_new + post_update + delete_update + no_change_post

        new = []
        update = []
        update.append(self.execution_metadata()) #add metadata row information
        for row in all_rows:
            if row.get("Email", "").lower() in email_lc_list:
                update.append(row)
            else:
                new.append(row)
        if update:
            self.log.info(f"Posting {len(update)} updates to Smartsheet...")
            sheet.update_rows(update, "Email")
        else:
            self.log.info("No updates to post")
        if new:
            self.log.info(f"Posting {len(new)} new rows to Smartsheet...")
            sheet.post_new_rows(new)
        else:
            self.log.info("No New to post")

#endregion

#region ---- Smartsheet Data ----
    def get_bamboo_data(self):
        """Retrieves the employee data from the "Employees_Bamboo Updated: ...." Smartsheet.
        Returns:
            List of "Employee" dataclass objects containing current employee information"""
        sheet = grid(self.BAMBOO_DATA_SS_ID)
        sheet.fetch_content()
        self.ss_employees = self._df_to_empl_obj(sheet.df)
        return self.ss_employees

    def _df_to_empl_obj(self, dataframe):
        """Takes a dataframe and turns each row into an employee object.
        Returns dict of employee objects as email:Employee"""
        employees = []
        for _, row in dataframe.iterrows():
            employee = Employee(
                first_name = row["preferredName"] if pd.notna(row["preferredName"]) and row["preferredName"] else row["firstName"],
                last_name = row["lastName"],
                email = row["emailAsText"].lower(),
                state = row["location"],
                region = self._normalize_region(row["location"], row["division"]),
                marketing_classification = "Dowbuilt Employee",
                company = str(self.HB_DB_COMPANY_ID)
            )
            employees.append(employee)
        self.log.info(f"Converted {len(employees)} Bamboo employees")
        #TODO: delete
        with open("bamboo.json", "w") as of:
            json.dump({emp.email:asdict(emp) for emp in employees}, of, indent=2)
        return employees
    
    def _normalize_region(self, location:str, division:str) -> str:
        """Normalize the region from Bamboo data to match the dropdown options in Hubspot"""
        if "Division 10" in division:
            division = location
        if division in self.regions:
            return division
        
        for region, locations in self.regions.items():
            if division in locations:
                return region
        
        return ""
     
    def get_hubspot_sheet_data(self):
        """Retrieves the current sheet data for the hubspot Employee sheet for update."""
        sheet = grid(self.HUBSPOT_SS_ID)
        sheet.fetch_content()
        sheet.df

#endregion

#region ---- Helper methods ----
    def load_config(self, file_path = "configs/config.json"):
        try:
            with open(file_path, "r") as inf:
                return json.load(inf)
        except Exception as e:
            self.log.error(f"ERROR: loading config file {e}")

    def _map_employees(self, employees:list[Employee]):
        """Maps employee objects by email
        Returns: dict as {email:Employee}"""
        return {emp.email:emp for emp in employees}
    
    def build_row(self, employee:Employee, action:str, update_time:datetime, removed=False):
        """build row to post to SS
        Params:
        action: String action of either : Updated, Archived, Created
        removed: Bool for "Removed" column checkbox"""
        return {
            "First Name":employee.first_name,
            "Last Name": employee.last_name,
            "State": employee.state,
            "Region": employee.region,
            "Email": employee.email,
            "Comments": f"{action}",
            "Latest Update": update_time,
            "Removed": removed,
        }
    
    def _get_counts(self):
        """Gets employee count from both sources"""
        hub = self.hub_client.get_employees()
        return len(hub), len(self.ss_employees)
    
    def execution_metadata(self):
        """Generates metadata about current execution and formats to Employee object for posting to SS"""
        hub, bb = self._get_counts()
        return {
            "Email": "Execution Metadata:",
            "Latest Update": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "Comments": f"{hub} hubspot employees synced from {bb} Bamboo employees",
        }
    
 #endregion

def main():
    hbs = HubspotEmployeeSync()
    hbs.sync()
main()


