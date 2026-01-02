# This file is the actual code for the Python runnable remove-orphaned-projects-from-conns
import dataiku
import logging
import pandas as pd
from dataiku.runnables import Runnable, utils, ResultTable
import plugin_utils.project_utils as project_utils

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None

    def run(self, progress_callback):
        
        logging.info("Starting 'Remove Orphaned PROJECT_KEYs from CONNs' Macro!")
        
        is_dry_run = self.config.get("is_dry_run")
        logging.info("DRY RUN is set to {}".format(str(is_dry_run)))
        
        action_status = "Not done (Dry run)" if is_dry_run else "Done"
        
        user_client = dataiku.api_client()
        user_auth_info = user_client.get_auth_info()
        admin_client = utils.get_admin_dss_client("creation1", user_auth_info)
        projects = admin_client.list_project_keys();
        
        conn_dict=[]
        delim=','
        
        conn_list=[]
        
        if self.config.get("doAll", None):
            conn_list=admin_client.list_connections()
        elif self.config.get("connection", None):
            conn_list.append(self.config.get("connection", None))
            
        for connection_name in conn_list:
            connection = admin_client.get_connection(connection_name) 
            connection_dict=connection.get_definition()
            dku_properties = connection_dict['params'].get('dkuProperties',"")

            action="NONE"
            status= 'Dry Run'


            allowed_in_projects_property = next(
                (prop for prop in dku_properties if prop['name'] == 'dku.security.allowedInProjects'), None)
            
            if (allowed_in_projects_property):
                currentListOfAllowedProjectsInConnection=allowed_in_projects_property["value"]

                allowListUpdated=[]
                cnt=0
                missingList=[]
                for p in currentListOfAllowedProjectsInConnection.split(delim):
                    if (p not in projects) and ("_*" not in p):
                        missingList.append(p)
                        cnt+=1
                    else:
                        allowListUpdated.append(p)
                
                if cnt > 0:
                    logging.info("For connection: " + connection_name + " found the following orphaned PROJECT_KEYS: " + delim.join(missingList))
                
                #if new length is different, process the udpate
                if (len(currentListOfAllowedProjectsInConnection.split(delim))!=len(allowListUpdated)):
                    action="UPDATE"
                    allowListUpdated = delim.join(allowListUpdated)

                    if not is_dry_run:
                        #Update allowedInProjects list
                        allowed_in_projects_property = next(
                        (prop for prop in dku_properties if prop['name'] == 'dku.security.allowedInProjects'), None)

                        #print(allowed_in_projects_property)
                        if allowed_in_projects_property:
                            allowed_in_projects_property['value'] = allowListUpdated
                            
                            connection.set_definition(connection_dict)
                            logging.info("Connection: ", connection.name, " updated!")
                            status="UPDATED"

                conn_dict.append({"Name":connection.name,"Remove Keys":cnt,"action":action,"status":status})
        
        return pd.DataFrame(conn_dict).to_html()
        
