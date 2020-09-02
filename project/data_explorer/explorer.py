#%%
class Get_blob_storage_content():
    """
    Generic class for accessing content stored in blob storage

    Parameters
    ----------
    root_path: root path of the azure blob storage system 
    container_name: blob storage container name
    account_name: name of the associated account
    sas_key: access key

    Attributes
    ----------
    blob_st_path_: root path updated with credentials

    Examples
    --------
    >>> STORAGE_ACCOUNT = 'sproeblob'
    >>> CONTAINER = 'sparkfiles'
    >>> SAS_KEY = 'XYZ'
    >>> blob_st_root_path = "wasbs://{container}@{storage_acct}.blob.core.windows.net"

    >>> get_blob_content_obj = Get_blob_st_content(blob_st_root_path, CONTAINER, STORAGE_ACCOUNT, SAS_KEY)

    """

    def __init__(self, root_path, container_name, account_name, sas_key=None):
        self.root_path_ = root_path
        self.container_name_ = container_name
        self.account_name_ = account_name
        self.key_ = sas_key

        spark.conf.set(
            "fs.azure.account.key.{storage_acct}.blob.core.windows.net".format(storage_acct=self.account_name_),
            "{access_key}".format(access_key=self.key_))

    def display_root_dir_content(self):
        try:
            blob_st_path = self.root_path_.format(container=self.container_name_, storage_acct=self.account_name_)
            self.blob_st_path_ = blob_st_path
            
            return dbutils.fs.ls(blob_st_path)
            
        except Exception as exc:
            #logger.exception('raised exception at {}: {}'.format(logger.name+'.'+self.build_data_frame_struct.__name__, exc))
            return('exception at display_root_dir_content: ', exc)
                
    def display_child_dir_content(self, child_dir_name):
        try:
            blob_st_child_path = self.blob_st_path_ + child_dir_name
            
            return dbutils.fs.ls(blob_st_child_path)
            
        except Exception as exc:
            #logger.exception('raised exception at {}: {}'.format(logger.name+'.'+self.build_data_frame_struct.__name__, exc))
            return('exception at display_child_dir_content: ', exc)
        
