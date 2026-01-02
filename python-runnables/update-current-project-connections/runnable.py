# This file is the actual code for the Python runnable update-current-project-connections
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
        
        logging.info("Starting 'Update Current Project Connections' Macro!")
        
        is_dry_run = self.config.get("is_dry_run")
        logging.info("DRY RUN is set to {}".format(str(is_dry_run)))
        
        # Initialize macro result table:
        result_table = ResultTable()
        result_table.add_column("conn", "Connection", "STRING")
        #result_table.add_column("type", "Type", "STRING")
        result_table.add_column("action", "Action", "STRING")
        result_table.add_column("action_status", "Action Status", "STRING")
        
        action_status = "Not done (Dry run)" if is_dry_run else "Done"
        
        user_client = dataiku.api_client()
        user_auth_info = user_client.get_auth_info()
        admin_client = utils.get_admin_dss_client("creation1", user_auth_info)
        
        if self.config.get("project_key", None):
            project = admin_client.get_project(self.config.get("project_key"))
        else:
            project = admin_client.get_project(self.project_key)
        
        
        project_permissions = project.get_permissions()
        group_names = [item['group'] for item in project_permissions["permissions"]]
        
        #logging.info("Project {} has the following group_names {}".format(str(project.get_summary()["projectKey"]),str(group_names)))
        
        conn_dict=[]
        
        for connection_name in admin_client.list_connections():
            connection = admin_client.get_connection(connection_name)
            conn_row = {}
            conn_row["Name"]=connection_name
            
            connection_dict = connection.get_definition()
            updated_connection_info = project_utils.update_connection_properties(connection_dict, group_names, project.get_summary()["projectKey"])
            #logging.info("Retruned Update: {} with Props {}".format(str( connection_name), str (updated_connection_info['params']['dkuProperties']) ))
            
            action="NONE"
            status= 'Dry Run'
            
            delim=','

            #Check if update is done
            orig = connection.get_definition()['params']['dkuProperties']
            upd = updated_connection_info['params']['dkuProperties']
            
            try:
                allowed_in_projects_property = next(
                (prop for prop in orig if prop['name'] == 'dku.security.allowedInProjects'), None)

                if (allowed_in_projects_property):
                    orig=allowed_in_projects_property["value"]
                    #logging.info('ORIGINAL', delim.join(sorted(orig.split(delim))) )

                    allowed_in_projects_property = next(
                        (prop for prop in upd if prop['name'] == 'dku.security.allowedInProjects'), None)

                    if (allowed_in_projects_property):
                        if (len(orig)>0 and len(upd)>0):
                            upd=allowed_in_projects_property["value"]
                            if (delim.join(sorted(orig.split(delim))) != delim.join(sorted(upd.split(delim)))):
                                action="UPDATE"
                        elif (len(orig)==0 and len(upd)>0):
                            action="UPDATE"
                else:
                    action="UPDATE"
            except Exception as exp:
                logging.info("Original {} ... Updated {}".format(str(orig),str(upd)))
                raise exp
            
            if not is_dry_run:
                if action=="UPDATE":
                    logging.info("Updating connection {}".format(str(connection_name)))
                    connection.set_definition(updated_connection_info)
                    status="UPDATED"
            
            conn_row["action"]=action
            conn_row["status"]=status
            conn_row["details"]=connection_dict['detailsReadability']['readableBy']
            conn_row["allowed groups"]=len(connection_dict['detailsReadability']['allowedGroups'])
            conn_dict.append(conn_row)
        
        
        #results_df = pd.DataFrame(conn_dict, columns=["Name", "action","status"])
        
        # Pass results to result table
        #for index, row in results_df.iterrows():
        #    result_table.add_record(list(row))
        
        #return result_table
        return pd.DataFrame(conn_dict).to_html()
        
