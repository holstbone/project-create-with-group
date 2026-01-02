from dataiku.runnables import utils
import dataiku

def do(payload, config, plugin_config, inputs):
    # Get user identity
    user_client = dataiku.api_client()
    user_auth_info = user_client.get_auth_info()

    # Get client with administrator privileges (required to list code environments)
    admin_client = utils.get_admin_dss_client("admin_macro_api", user_auth_info)
    
    # List python or R code environments
    envs = list()
    
    if payload["parameterName"]=="python_environment":
        for env in admin_client.list_code_envs():
            if env["envLang"]=="PYTHON" and env["deploymentMode"]=="DESIGN_MANAGED":
                envs.append({"value": env["envName"], "label": env["envName"]})
        
    elif payload["parameterName"]=="r_environment":
        for env in admin_client.list_code_envs():
            if env["envLang"]=="R" and env["deploymentMode"]=="DESIGN_MANAGED":
                envs.append({"value": env["envName"], "label": env["envName"]})
    
    return {"choices": envs}
