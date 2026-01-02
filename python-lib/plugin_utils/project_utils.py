import dataikuapi
from joblib import Parallel, delayed
from joblib.externals.loky import get_reusable_executor
import numpy as np
import multiprocessing

def remove_dataset_ref(admin_client, project_key):
    p = admin_client.get_project(project_key)
    print("cleaning datasets")
    for dataset in p.list_datasets():
        print("processing-",dataset)
        #if not dataset['managed'] and not dataset['type'] == 'UploadedFiles':
        if not dataset['type'] == 'UploadedFiles':
            dataset_settings = p.get_dataset(dataset['name']).get_settings()
            raw_params = dataset_settings.get_raw_params()
            if isinstance(dataset_settings, dataikuapi.dss.dataset.SQLDatasetSettings):
                print("Clearing params for SQL dataset %s" % dataset['name'])
                if 'connection' in raw_params:
                    raw_params['connection'] = "z_admin_SQL_conn_DO_NOT_USE"
                if 'catalog' in raw_params:
                    raw_params['catalog'] = ""
                if 'schema' in raw_params:
                    raw_params['schema'] = ""
                if 'query' in raw_params:
                    raw_params['query'] = ""
                raw_params['table'] = ""
            elif isinstance(dataset_settings, dataikuapi.dss.dataset.FSLikeDatasetSettings):
                print("Clearing params for FileSystem dataset %s" % dataset['name'])
                if 'connection' in raw_params:
                    raw_params['connection'] = "z_admin_S3_conn_DO_NOT_USE"
                if 'path' in raw_params:
                    raw_params['path'] = "/blank"
                if 'bucket' in raw_params:
                    raw_params['bucket'] = "/"
                raw_params['filesSelectionRules'] = {'excludeRules': [], 'explicitFiles': [], 'includeRules': [], 'mode': 'EXPLICIT_SELECT_FILES'}
            else:
                print('skipped dataset-', dataset)
            
            dataset_settings.save()

def clear_project_permissions(admin_client, project_key):
    """
    Clears ALL project permissions.
    """

    # Retrieve project permissions
    project = admin_client.get_project(project_key)
    permissions = project.get_permissions()

    # Clear permissions, dashboard users, and dashboard authorizations
    permissions["permissions"] = list()
    permissions["additionalDashboardUsers"]["users"] = list()
    permissions["dashboardAuthorizations"]["allAuthorized"] = False
    permissions["dashboardAuthorizations"]["authorizations"] = list()
    project.set_permissions(permissions=permissions)

    return True


def set_project_owner(admin_client, project_key, user):
    # Retrieve project permissions
    project = admin_client.get_project(project_key)
    permissions = project.get_permissions()

    # Set user and owner
    permissions["owner"] = user
    project.set_permissions(permissions=permissions)

    return True


def get_default_envs(admin_client, project_key):
    # Get project settings and instance settings
    project = admin_client.get_project(project_key)

    project_settings = project.get_settings().settings
    general_settings = admin_client.get_general_settings()

    # Get default instance code envs (for convenience, might be required)
    instance_python_env = general_settings.settings["codeEnvs"].get("defaultPythonEnv", None)
    instance_r_env = general_settings.settings["codeEnvs"].get("defaultREnv", None)

    if not instance_python_env:
        instance_python_env = "Builtin Python Env"
    if not instance_r_env:
        instance_r_env = "Builtin R Env"

    # Get default project envs
    python_env_mode = project_settings["settings"]["codeEnvs"]["python"]["mode"]
    python_override = project_settings["settings"]["codeEnvs"]["python"]["preventOverride"]

    if python_env_mode == "INHERIT":
        python_env = instance_python_env
    elif python_env_mode == "USE_BUILTIN_MODE":
        python_env = "Builtin Python Env"
    else:
        python_env = project_settings["settings"]["codeEnvs"]["python"]["envName"]

    r_env_mode = project_settings["settings"]["codeEnvs"]["r"]["mode"]
    r_override = project_settings["settings"]["codeEnvs"]["r"]["preventOverride"]

    if r_env_mode == "INHERIT":
        r_env = instance_r_env
    elif r_env_mode == "USE_BUILTIN_MODE":
        r_env = "Builtin R Env"
    else:
        r_env = project_settings["settings"]["codeEnvs"]["r"]["envName"]

    return python_env_mode, python_env, python_override, r_env_mode, r_env, r_override


def set_default_envs(admin_client, project_key,
                     python_env_mode, r_env_mode,
                     prevent_python_override, prevent_r_override,
                     python_env=None, r_env=None):
    # Set code env defaults
    python = {'mode': python_env_mode, 'preventOverride': prevent_python_override}
    if python_env_mode == "EXPLICIT_ENV":
        python["envName"] = python_env

    r = {'mode': r_env_mode, 'preventOverride': prevent_r_override}
    if r_env_mode == "EXPLICIT_ENV":
        r["envName"] = r_env

    # set project settings 
    project = admin_client.get_project(project_key)

    settings = project.get_settings()
    settings.settings["settings"]["codeEnvs"]["python"] = python
    settings.settings["settings"]["codeEnvs"]["r"] = r
    settings.save()

    return True


def remove_project_permissions(admin_client, project_key, users=list(), groups=list()):
    """
    Removes permissions for a specific list of users and groups.
    """

    # Retrieve project permissions
    project = admin_client.get_project(project_key)
    permissions = project.get_permissions()

    # Remove requested permissions
    clean_permissions = list()

    for permission in permissions["permissions"]:
        user = permission.get("user", None)
        group = permission.get("group", None)

        if (user not in users) and (group not in groups):
            clean_permissions.append(permission)

    permissions["permissions"] = clean_permissions
    project.set_permissions(permissions=permissions)
    return True


def set_project_permissions(admin_client, project_key, new_permissions, users=list(), groups=list()):
    # Retrieve project permissions
    project = admin_client.get_project(project_key)
    permissions = project.get_permissions()

    # Remove from current project permissions any user and group present in the function request
    clean_permissions = list()

    for permission in permissions["permissions"]:
        user = permission.get("user", None)
        group = permission.get("group", None)

        if (user not in users) and (group not in groups):
            clean_permissions.append(permission)

    permissions["permissions"] = clean_permissions

    # Set permissions for users and groups
    for user in users:
        user_permissions = new_permissions.copy()
        user_permissions["user"] = user
        permissions["permissions"].append(user_permissions)

    for group in groups:
        group_permissions = new_permissions.copy()
        group_permissions["group"] = group
        permissions["permissions"].append(group_permissions)

    project.set_permissions(permissions=permissions)
    return True


def is_usermapping_configured(admin_client, project_key):
    """
    Returns true if there is a project-level user-mapping rule for this specific project key.
    Returns false otherwise.
    """
    global_settings = admin_client.get_general_settings()
    if len(global_settings.get_impersonation_rules(project_key=project_key, scope="PROJECT", is_user=True)):
        return True
    else:
        return False

def update_connection_properties(connection_dict, project_allowed_groups, project_name, property_name='dku.security.allowedInProjects'):
    # Extract the 'dkuProperties' list from the connection_dict
    dku_properties = connection_dict['params'].get('dkuProperties', [])
    connection_allowed_groups = connection_dict['detailsReadability']['allowedGroups']
    # Check if the 'roleName' property exists in dkuProperties
    if any(item in project_allowed_groups for item in connection_allowed_groups):
        # Check if 'dku.security.allowedInProjects' property exists
        allowed_in_projects_property = next(
            (prop for prop in dku_properties if prop['name'] == property_name), None)
        #print(connection_dict)
        if allowed_in_projects_property:
            project_list = allowed_in_projects_property['value']
            project_set = set(project_list.split(','))
            project_set.add(f'{project_name}')
            new_project_set = ','.join(project_set)
            allowed_in_projects_property['value'] = new_project_set
        else:
            dku_properties.append({'name':property_name, 'value':project_name, 'secret': False})

    updated_connection_info = connection_dict
    return updated_connection_info


def update_project_permissions(project):
    project_metadata = project.get_metadata()

    # Set Project Tags
    project_metadata['tags'] = ['%s' % (config["groupName"]), 'project-creation-macro']

    # Custom Field to use when duplicating projects
    project_metadata['customFields'] = {'groupName': '%s' % (config["groupName"])}
    project.set_metadata(project_metadata)

    # Move the project to the current project folder, passed in the config as _projectFolderId
    project.move_to_folder(admin_client.get_project_folder(config['_projectFolderId']))

    project_permissions = project.get_permissions()

    permission = {'group': '%s' % (config["groupName"]), 'admin': False, 'writeProjectContent': True,
                  'readProjectContent': True, 'readDashboards': True, "exportDatasetsData": True,
                  "manageDashboardAuthorizations": True, "manageExposedElements": True}

    return project_permissions['permissions'].append(permission)


# -----------------------------------------------------------------------------
# Add projectkeys to all connections with the name of the role
def add_pkeys_to_connections_parallel(admin_client, project_key, role_names, connections):
    for connection_name in connections:
        connection = admin_client.get_connection(connection_name)
        connection_dict = connection.get_definition()
        updated_connection_info = update_connection_properties(connection_dict, role_names, project_key)
        connection.set_definition(updated_connection_info)
    return

def add_pkeys_to_connections(admin_client, project_key, role_names):
    n_jobs = multiprocessing.cpu_count() - 1
    connections_l = admin_client.list_connections_names(connection_type = "all")
    arrays = np.array_split(connections_l, n_jobs)
    results = Parallel(n_jobs=n_jobs)\
        (delayed(add_pkeys_to_connections_parallel)\
        (admin_client, project_key, role_names, connections) for connections in arrays)
    get_reusable_executor().shutdown(wait=True)
    return

# -----------------------------------------------------------------------------
# EOF
