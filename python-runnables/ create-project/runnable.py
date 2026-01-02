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
        # Get the identity of the end DSS user
        user_client = dataiku.api_client()
        user_auth_info = user_client.get_auth_info()

        # Automatically create a privileged API key and obtain a privileged API client
        # that has administrator privileges.
        admin_client = utils.get_admin_dss_client("creation1", user_auth_info)

        # The project creation macro must create the project. Therefore, it must first assign
        # a unique project key. This helper makes this easy
        project_key = utils.make_unique_project_key(admin_client, self.config["projectName"])

        # The macro must first perform the actual project creation.
        # We pass the end-user identity as the owner of the newly-created project
        print("Creating project")
        admin_client.create_project(project_key, self.config["projectName"], 'admin')

        print("Configuring project")
        project = admin_client.get_project(project_key)
        project_metadata = project.get_metadata()
        project_metadata['tags'] = ['%s' %(self.config["groupName"]),'project-creation-macro']
        
        # Custom Field to use when creating connections
        project_metadata['customFields'] ={'groupName':'%s' %(self.config["groupName"])}
        project.set_metadata(project_metadata)

        # Move the project to the current project folder, passed in the config as _projectFolderId
        project.move_to_folder(admin_client.get_project_folder(self.config['_projectFolderId']))

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

        for connection_name in admin_client.list_connections():
            connection = admin_client.get_connection(connection_name)
            connection_dict = connection.get_definition()
            updated_connection_info = project_utils.update_connection_properties(connection_dict, role_names, project_key)
            connection.set_definition(updated_connection_info)

        # A project creation macro must return a JSON object containing a `projectKey` field with the newly-created
        # project key
        return json.dumps({"projectKey": project_key})
    
    
