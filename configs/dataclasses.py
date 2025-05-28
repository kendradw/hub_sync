from dataclasses import dataclass

@dataclass
class Employee:
    first_name:str
    last_name:str
    email:str
    state:str
    region:str
    marketing_classification:str
    company:str
    hub_id:int = None



