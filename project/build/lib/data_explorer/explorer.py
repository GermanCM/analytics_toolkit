#%%
class Blob_storage_info_provider():
    """
    Generic class for accessing content stored in blob storage

    Parameters
    ----------
    root_path: string 
        root path of the azure blob storage system 
    container_name: string 
        blob storage container name
    account_name: string 
        name of the associated account
    sas_key: string 
        access key

    Attributes
    ----------
    blob_st_path_: string 
        root path updated with credentials

    Examples
    --------
    >>> STORAGE_ACCOUNT = 'sproeblob'
    >>> CONTAINER = 'sparkfiles'
    >>> SAS_KEY = 'XYZ'
    >>> blob_st_root_path = "wasbs://{container}@{storage_acct}.blob.core.windows.net"

    >>> get_blob_content_obj = Blob_storage_info_provider(blob_st_root_path, CONTAINER, STORAGE_ACCOUNT, SAS_KEY)
    """

    def __init__(self, root_path, container_name, account_name, current_env = 'local', sas_key=None):
        self.root_path_ = root_path
        self.container_name_ = container_name
        self.account_name_ = account_name
        self.key_ = sas_key
        self.current_env_ = current_env

        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils

        spark = SparkSession.builder.getOrCreate()

        dbutils = DBUtils(spark.sparkContext)
        self.dbutils_ = dbutils

        spark.conf.set(
            "fs.azure.account.key.{storage_acct}.blob.core.windows.net".format(storage_acct=self.account_name_),
            "{access_key}".format(access_key=self.key_))

    def display_root_dir_content(self):
        """
        Provides the content of the root directory

        Examples
        --------
        >>> get_blob_content_obj = Blob_storage_info_provider(blob_st_root_path, CONTAINER, STORAGE_ACCOUNT, SAS_KEY)
        >>> get_blob_content_obj.display_root_dir_content()

        """
        try:
            blob_st_path = self.root_path_.format(container=self.container_name_, storage_acct=self.account_name_)
            self.blob_st_path_ = blob_st_path
            
            return self.dbutils_.fs.ls(blob_st_path)
            
        except Exception as exc:
            #logger.exception('raised exception at {}: {}'.format(logger.name+'.'+self.build_data_frame_struct.__name__, exc))
            return('exception at display_root_dir_content: ', exc)
                
    def display_child_dir_content(self, child_dir_name):
        """
        Generic class for accessing content stored in blob storage

        Parameters
        ----------
        child_dir_name: string 
            name of the child directory of the azure blob storage system to explore

        >>> get_blob_content_obj = Blob_storage_info_provider(blob_st_root_path, CONTAINER, STORAGE_ACCOUNT, SAS_KEY)
        >>> get_blob_content_obj.display_child_dir_content("/rsi")
        """
        try:
            blob_st_child_path = self.blob_st_path_ + child_dir_name
            
            return self.dbutils_.fs.ls(blob_st_child_path)
            
        except Exception as exc:
            #logger.exception('raised exception at {}: {}'.format(logger.name+'.'+self.build_data_frame_struct.__name__, exc))
            return('exception at display_child_dir_content: ', exc)


#%%
class Dataset_info_provider():
    """
    Class for accessing content of a dataframe (pandas dataframe, spark dataframe, koalas dataframe, etc)

    Parameters
    ----------
    dataframe: pandas, koalas, spark dataframe
        dataset containing the info we want to retrieve 
    current_env: 
        whether 'local' or 'cloud' (in case we want to run this code on a cloud provider like Azure Databricks)

    Examples
    --------
    >>> dataset_info_provider = Dataset_info_provider(this_mov_type_movs)
    >>> dataset_info_provider.dataframe_.count()
    22304761
    >>> dataset_info_provider.get_unique_col_vals_from_spark_df('categoryDescription')
    ['EFECTIVO', 'OPERACIONES PROPIAS DE ENTIDAD', 'TRANSFERENCIAS DE ENTRADA']
    
    """

    def __init__(self, dataframe, current_env = 'local'):
        self.dataframe_ = dataframe
        self.current_env_ = current_env

        if self.current_env_ == 'local':
            from pyspark.sql import SparkSession
            from pyspark.dbutils import DBUtils

            spark = SparkSession.builder.getOrCreate()
            self.spark_ = spark

    def get_unique_attribute_values(self, column_name):
        """
        returns the unique values found in a dataframe column

        Parameters
        ----------
        column_name: string 
            column with the desired values to explore

        Returns
        -------
            array: array of unique values from 'column_name'
        
        Examples
        --------
        >>> import pandas as pd
        >>> from data_explorer import explorer

        >>> test_df = pd.DataFrame({'col_A': [1, 2, 2, 7], 'col_B': ['aa', 'bb', 'cc', 'dd'], 'col_C': [2, 43, 2, 87]})
        >>> explorer_obj = explorer.Dataset_info_provider(test_df)

        >>> unique_col_A_values = explorer_obj.get_unique_attribute_values('col_A')
        >>> unique_col_A_values
        array([1, 2, 7])
        
        """
        try:
            if ('pandas' in str(type(self.dataframe_))) | ('koalas' in str(type(self.dataframe_))):
                return self.dataframe_[column_name].unique()

            elif 'spark' in str(type(self.dataframe_)):
                return [x[column_name] for x in self.dataframe_.select(column_name).distinct().collect()]
    
        except Exception as exc:
            return exc


    def filter_dataframe_by_column_value(self, column_name, value):
        """filters rows of a dataframe (the one in self.dataframe_) given a column name to filter on and the desired value;
           it accepts a spark dataframe, pandas dataframe or koalas dataframe
        Args:
            column_name (string): attribute to filter on
            value (object): value of the same type of the column values

        Returns:
            dataframe: the filtered dataframe; it could be an empty dataframe
        """
        try:
            dataframe_type = type(self.dataframe_)

            if 'spark' in str(dataframe_type):
                return  self.dataframe_.filter(self.dataframe_[column_name]==value)

            else:
                value_mask = self.dataframe_[column_name] == value
                return self.dataframe_[value_mask]

        except Exception as exc:
            return exc 
