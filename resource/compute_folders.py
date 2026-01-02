import dataiku

def get_folder_structure(folder, folders_list):
    
    # Add to folders dictionary with [folder path -> folder object]
    folder_path = folder.get_path()
    folders_list.append({"value": folder.id, "label": folder_path})

    for child_folder in folder.list_child_folders():
        get_folder_structure(child_folder, folders_list)
    return

def do(payload, config, plugin_config, inputs):
    
    client = dataiku.api_client()
    root_folder = client.get_root_project_folder()
    folders_list = list()

    get_folder_structure(root_folder, folders_list)
    
    return {"choices": folders_list}
