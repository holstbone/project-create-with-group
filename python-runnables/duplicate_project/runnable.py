# This file is the actual code for the Python runnable duplicate-project
from dataiku.runnables import Runnable, utils
import dataiku

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
        # Get user identity
        user_client = dataiku.api_client()
        user_auth_info = user_client.get_auth_info()
        
        # Retrieve client with administrator permissions (required to duplicate the project)
        admin_client = utils.get_admin_dss_client("admin_macro_api", user_auth_info)
        
        # Retrieve project and duplicate
        project = admin_client.get_project(self.project_key)
        
        
        project_name = self.config["project_name"]
        project_key = self.config.get("project_key", None)
        
        if project_key:
            project_key = utils.make_unique_project_key(admin_client, project_key)
        else:
            project_key = utils.make_unique_project_key(admin_client, project_name)
        
        #TODO Add check for ensuring there is a folder selected
        project_folder_id = self.config["project_folder_id"]
        print(project_folder_id)
        folder = admin_client.get_project_folder(project_folder_id)
        
        duplication_mode = self.config["duplication_mode"]
        export_analysis_models = self.config["export_analysis_models"]
        export_saved_models = self.config["export_saved_models"]
        
        project_metadata = project.get_metadata()
        project_permissions = project.get_permissions()
        role_name = [item['group'] for item in project_permissions["permissions"]]
        groups = [item['group'] for item in project_permissions["permissions"]]
        
        for connection_name in admin_client.list_connections():
            connection = admin_client.get_connection(connection_name)
            connection_dict = connection.get_definition()
            updated_connection_info = project_utils.update_connection_properties(connection_dict, role_name, project_key)
            connection.set_definition(updated_connection_info)

        project.duplicate(target_project_key=project_key, 
                          target_project_name=project_name, 
                          duplication_mode=duplication_mode, 
                          export_analysis_models=export_analysis_models, 
                          export_saved_models=export_saved_models, 
                          target_project_folder=folder)
        
         # Clear project permissions and make user running macro the owner
        project_utils.clear_project_permissions(admin_client, project_key)
        project_utils.set_project_owner(admin_client, project_key, 'admin')
        
        print("Configuring project")
        project = admin_client.get_project(project_key)
        project_metadata = project.get_metadata()
        project_metadata['tags'] = ['%s' %(role_name),'project-creation-macro']

        project_permissions = project.get_permissions()

        # Set Project Permissions 
        for group in groups:
            permission = {
                'group': '%s' % group,
                'admin': False,
                'writeProjectContent': True,
                'readProjectContent': True,
                'readDashboards': True,
                'exportDatasetsData': True,
                'manageDashboardAuthorizations': True,
                'manageExposedElements': True
            }
            project_permissions['permissions'].append(permission)
        project.set_permissions(project_permissions)

        return "Project duplicated successfully with project key: {}.".format(project_key)
        
