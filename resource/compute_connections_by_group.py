from dataiku.runnables import utils
import dataiku

def do(payload, config, plugin_config, inputs):

        # Get user identity
    user_client = dataiku.api_client()
    user_auth_info = user_client.get_auth_info()

    # Get client with administrator privileges (required to list code environments)
    admin_client = utils.get_admin_dss_client("admin_macro_api", user_auth_info)
    project_permissions = admin_client.get_default_project().get_permissions()

    group_names = [item['group'] for item in project_permissions["permissions"]]

    group_names_new={}

    for groupName in group_names:
        groupName_list=["sandbox"]
        i=2

        while i < len(groupName.split("_"))-1:
            groupName_list.append(groupName.split("_")[i].lower())
            i+=1

        groupName_end="-".join(groupName_list)
        group_names_new[groupName]=groupName_end

    print(group_names_new.keys())
    for g in group_names_new:
        print(g,group_names_new[g])
    
    
    values = list()

    for c in admin_client.list_connections():
        for g_key in group_names_new:
            #if c.startswith(g_key) and (group_names_new[g_key] in c):
            if c.startswith(g_key) and ("sandbox" in c):

                values.append({"value": c, "label": c})
        
        #if c.startswith("SSFCMGOASIS_DEV_MEDICAL_ANALYST_BR") and ("sandbox" in c):
        #    values.append({"value": c, "label": c})
    
    return {"choices": values}
