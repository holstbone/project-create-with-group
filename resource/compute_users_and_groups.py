from dataiku.runnables import utils
import dataiku

def do(payload, config, plugin_config, inputs):
    # Get user identity
    user_client = dataiku.api_client()
    user_auth_info = user_client.get_auth_info()

    # Get client with administrator privileges (required to list users and groups)
    admin_client = utils.get_admin_dss_client("admin_macro_api", user_auth_info)
    
    # List users or groups
    if payload["parameterName"]=="users":
        members = [{"label": user["displayName"], "value": user["login"]} for user in admin_client.list_users()]
        
    elif payload["parameterName"]=="groups":
        members = [{"label": group["name"], "value": group["name"]} for group in admin_client.list_groups()]
        

    
    
    return {"choices": members}
