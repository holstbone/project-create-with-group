# This file is the actual code for the Python runnable update-all-groups
from dataiku.runnables import Runnable
import plugin_utils.project_utils as project_utils

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
        import dataiku
        from dataiku.runnables import utils
        user_client = dataiku.api_client()
        user_auth_info = user_client.get_auth_info()
        admin_client = utils.get_admin_dss_client("creation1", user_auth_info)
        dss_projects = admin_client.list_project_keys()

        for dss_project in dss_projects: 
            project = admin_client.get_project(dss_project)
            project_permissions = project.get_permissions()
            group_names = [item['group'] for item in project_permissions["permissions"]]
            print(group_names)


            for connection_name in admin_client.list_connections():
                connection = admin_client.get_connection(connection_name)
                connection_dict = connection.get_definition()
                updated_connection_info = project_utils.update_connection_properties(connection_dict, group_names, dss_project)
                print(updated_connection_info)
                connection.set_definition(updated_connection_info)
                
        return "All Project Updated"
