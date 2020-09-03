#%%
class Datasets_checker():
    """
    Generic class for testing datasets content

    Parameters
    ----------
    root_path: datasets list

    Attributes
    ----------
    
    """

    def __init__(self, datasets):
        self.datasets_ = datasets


    def compare_datasets(self):
        """Compares the content of 2 dataframes 

        Returns:
            bool: whether the dataframes are the same or not

        Examples
        --------
        >>> import pandas as pd 
        >>> dataframe_1 = pd.DataFrame({'a': [1, 2, 3], 'b': [3, 2, 3]})
        >>> dataframe_2 = pd.DataFrame({'a': [1, 2, 3], 'b': [3, 2, 3]})
        >>> dfs_list = [dataframe_1, dataframe_2]
        >>> datasets_checker_obj = Datasets_checker(dfs_list)
        >>> datasets_checker_obj.compare_datasets()
        True
        
        >>> dataframe_1 = pd.DataFrame({'a': [2, 2, 3], 'b': [3, 2, 3]})
        >>> dataframe_2 = pd.DataFrame({'a': [1, 2, 3], 'b': [3, 2, 3]})
        >>> dfs_list = [dataframe_1, dataframe_2]
        >>> datasets_checker_obj = Datasets_checker(dfs_list)
        >>> datasets_checker_obj.compare_datasets()
        False
        """
        try:
            assert len(self.datasets_)>= 2, 'not enough datasets to compare'
            if list(self.datasets_[0].reset_index().to_dict().values())[1:] == \
                list(self.datasets_[1].reset_index().to_dict().values())[1:]:
                return True
            else:
                return False
                
            'dataframes content is different'

        except Exception as exc:
            return exc 
