import dataiku
from dataiku.runnables import Runnable
from dataiku.runnables import utils
import json
import plugin_utils.project_utils as project_utils

class MyRunnable(Runnable):

    def __init__(self, unused, config, plugin_config):
        # Note that, as all macros, it receives a first argument
        # which is normally the project key, but which is irrelevant for project creation macros
        self.config = config

    def get_progress_target(self):
        return None


    def run(self, progress_callback):
        # Get user identity
        user_client = dataiku.api_client()
        user_auth_info = user_client.get_auth_info()
        
        # Retrieve client with administrator permissions (required to duplicate the project)
        admin_client = utils.get_admin_dss_client("admin_macro_api", user_auth_info)
        
        
        
        project_name = self.config["project_name"]
        project_key = self.config.get("projectKey", None)
        
        if project_key:
            project_key = utils.make_unique_project_key(admin_client, project_key)
        else:
            project_key = utils.make_unique_project_key(admin_client, project_name)
    
        #TODO Add check for ensuring there is a folder selected
        project_folder_id = self.config['_projectFolderId']#self.config["project_folder_id"]
        print(project_folder_id)
        folder = admin_client.get_project_folder(project_folder_id)
        
        duplication_mode = 'NONE'#self.config["duplication_mode"]
        export_analysis_models = False # self.config["export_analysis_models"]
        export_saved_models = False # self.config["export_saved_models"]
        

        # Retrieve project and duplicate
        gallery_project = admin_client.get_project(self.config["galleryProjectName"])

        

        project_permissions = dict()
        
        project_permissions['permissions'] = [
            {
            'group': '%s' % self.config["groupName"],
            'admin': False,
            'writeProjectContent': True,
            'readProjectContent': True,
            'readDashboards': True,
            'exportDatasetsData': True,
            'manageDashboardAuthorizations': True,
            'manageExposedElements': True
        }
        ]
        
        #TODO Add ProjectKey to all of the connection with the name of the role. 
        role_names = [item['group'] for item in project_permissions["permissions"]]
      
        print("Fixing connection permissions")
        # Before duplicating, add project key to connection security
        for connection_name in admin_client.list_connections():
            connection = admin_client.get_connection(connection_name)
            connection_dict = connection.get_definition()
            updated_connection_info = project_utils.update_connection_properties(connection_dict, role_names, project_key)
            connection.set_definition(updated_connection_info)
            
            
        
        print("duplicating project")
        
        
        gallery_project.duplicate(target_project_key=project_key, 
                          target_project_name=project_name, 
                          duplication_mode=duplication_mode, 
                          export_analysis_models=export_analysis_models, 
                          export_saved_models=export_saved_models, 
                          target_project_folder=folder)
        print("done duplicating project")
        
         # Clear project permissions and make user running macro the owner
        project_utils.clear_project_permissions(admin_client, project_key)
        project_utils.set_project_owner(admin_client, project_key, 'admin')

        print("Configuring project")
        project = admin_client.get_project(project_key)
        project_metadata = project.get_metadata()
        project_metadata['tags'] = ['%s' %(self.config["groupName"]),'project-creation-macro']
        
        # Custom Field to use when creating connections
        project_metadata['customFields'] ={'groupName':'%s' %(self.config["groupName"])}
        project.set_metadata(project_metadata)
        
        project.set_permissions(project_permissions)
  
        
        '''
        print("duplicating project")
        
        
        gallery_project.duplicate(target_project_key=project_key, 
                          target_project_name=project_name, 
                          duplication_mode=duplication_mode, 
                          export_analysis_models=export_analysis_models, 
                          export_saved_models=export_saved_models, 
                          target_project_folder=folder)

        print("done duplicating project")
        
         # Clear project permissions and make user running macro the owner
        project_utils.clear_project_permissions(admin_client, project_key)
        project_utils.set_project_owner(admin_client, project_key, 'admin')

        print("Configuring project")
        project = admin_client.get_project(project_key)
        project_metadata = project.get_metadata()
        project_metadata['tags'] = ['%s' %(self.config["groupName"]),'project-creation-macro']
        
        # Custom Field to use when creating connections
        project_metadata['customFields'] ={'groupName':'%s' %(self.config["groupName"])}
        project.set_metadata(project_metadata)


        project_permissions = project.get_permissions()

        # Set Project Permissions 
        permission = {
            'group': '%s' % self.config["groupName"],
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
        
        #TODO Add ProjectKey to all of the connection with the name of the role. 
        role_names = [item['group'] for item in project_permissions["permissions"]]
      
        print("Fixing connection permissions")
        # Before duplicating, add project key to connection security
        for connection_name in admin_client.list_connections():
            connection = admin_client.get_connection(connection_name)
            connection_dict = connection.get_definition()
            updated_connection_info = project_utils.update_connection_properties(connection_dict, role_names, project_key)
            connection.set_definition(updated_connection_info)
        '''
        return json.dumps({"projectKey": project_key})
