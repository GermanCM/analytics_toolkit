class Data_extractor():
    """
    Class with helper functions to read data from several formats into a dataframe; files can be in the cloud or locally stored

    Examples
    --------
    >>> from data_extractor import extractor
    >>> extractor_obj = extractor.Data_extractor_from_cloud()
   
    """

    def __init__(self):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        self.spark_ = spark

        self.dataframe_ = None

    def read_remote_data(self, data_file_format, data_file_path, *extra_params):
        """
        Returns a dataframe with the data found in the file path

        Parameters
        ----------
        data_file_format: string 
            'parquet', 'csv', 'json', etc

        data_file_path: string
            string path allocating the data files

        Returns
        -------
            dataframe: dataset containing the retrieved data from data_file_path
        
        Examples
        --------
        >>> from data_extractor import extractor
        >>> extractor_obj = extractor.Data_extractor_from_cloud()

        >>> from_parquet_df = extractor_obj.spark_.read.parquet('/mnt/analytics/pai/CardMovementNew')
        >>> from_parquet_df.count()
        84855294
        
        """
        try:
            if data_file_format=='parquet':
                self.dataframe_ = self.spark_.read.parquet(data_file_path)

            if data_file_format=='csv':
                self.dataframe_ = self.spark_.read.csv(data_file_path)

            if data_file_format=='json':
                self.dataframe_ = self.spark_.read.json(data_file_path)
            
            return self.dataframe_
            
        except Exception as exc:
            return exc
