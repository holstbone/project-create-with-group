# This file is the actual code for the Python runnable update-current-app-connections
import dataiku
import logging
import pandas as pd
from dataiku.runnables import Runnable, utils, ResultTable
import plugin_utils.project_utils as project_utils
import json

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

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
        
        #logging.info("Starting 'Update Current App Connections' Macro!")
        
        is_dry_run = self.config.get("is_dry_run")
        logging.info("DRY RUN is set to {}".format(str(is_dry_run)))
        
        action_status = "Not done (Dry run)" if is_dry_run else "Done"
        
        user_client = dataiku.api_client()
        user_auth_info = user_client.get_auth_info()
        admin_client = utils.get_admin_dss_client("creation1", user_auth_info)
        
        if self.config.get("project_key", None):
            project = admin_client.get_project(self.config.get("project_key"))
        else:
            project = admin_client.get_project(self.project_key)
        
        forceConnection=self.config.get("connection", None)
        
        result="Connection is not updated."
        
        if forceConnection:
            settings = project.get_settings()
            settings.get_raw()['settings']['datasetsCreationSettings']['useGlobal']=False
            settings.get_raw()['settings']['datasetsCreationSettings']['forcedPreferedConnection']=forceConnection
            settings.get_raw()['settings']['datasetsCreationSettings']['preferedStorageFormats']="PARQUET_HIVE,CSV_ESCAPING_NOGZIP_FORHIVE,CSV_EXCEL_GZIP"
            settings.save()
            result="Setting connection " + forceConnection + "as default!"
        
        
        
        return result
        
