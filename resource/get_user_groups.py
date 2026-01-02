import dataiku
import dataikuapi
from dataiku.runnables import utils


def do(payload, config, plugin_config, inputs):
  
  user_client = dataiku.api_client()
  user_auth_info = user_client.get_auth_info()

  # Automatically create a privileged API key and obtain a privileged API client
  # that has administrator privileges.
  admin_client = utils.get_admin_dss_client("creation1", user_auth_info)
    
  # user_groups is a list of dict. Each group contains at least a "name" attribute
  user_groups = user_client.get_own_user().get_settings().get_raw().get('groups')
  choices=[]
    
  for group in user_groups:
    new_val={}
    new_val["value"]=group
    new_val["label"]=group
    choices.append(new_val)
  
  print(str(choices))
  return {"choices": choices}
