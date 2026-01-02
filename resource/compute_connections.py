from dataiku.runnables import utils
import dataiku

def do(payload, config, plugin_config, inputs):
    # Get user identity
    user_client = dataiku.api_client()
    user_auth_info = user_client.get_auth_info()

    # Get client with administrator privileges (required to list code environments)
    admin_client = utils.get_admin_dss_client("admin_macro_api", user_auth_info)
    
    values = list()
    conns=[]
    
    ds = admin_client.get_default_project().list_datasets()

    for d in ds:
        if (d.connection not in conns):
            conns.append(d.connection)
    
    for c in conns:
        values.append({"value": c, "label": c})
    
    return {"choices": values}
