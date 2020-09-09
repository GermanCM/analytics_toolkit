#%%
import unittest
import pytest

#%%
class CodeTests(unittest.TestCase):
    
    def test_array_length(self):
        test_array = [1, 2, 3]
        l = len(test_array)
        self.assertEqual(l, 3)

    def test_display_root_dir_content(self):
        from data_explorer import explorer 

        STORAGE_ACCOUNT = 'sproeblob'
        CONTAINER = 'sparkfiles'
        SAS_KEY = 't5NtMOUwxTaWQZaaffV7nTNL/AKUveQuntrNyI+X6c7zJrKcIjOW4ih6WFkEL7hJkmO7pslFANMJcWiOvAG5OA=='
        blob_st_root_path = "wasbs://{container}@{storage_acct}.blob.core.windows.net"
        
        get_blob_content_obj = explorer.Blob_storage_info_provider(blob_st_root_path, CONTAINER, STORAGE_ACCOUNT, SAS_KEY)
        print(get_blob_content_obj.display_root_dir_content())

    def test_get_unique_attribute_values(self):
        import pandas as pd
        from data_explorer import explorer
        import numpy as np

        test_df = pd.DataFrame({'col_A': [1, 2, 2, 7], 'col_B': ['aa', 'bb', 'cc', 'dd'], 'col_C': [2, 43, 2, 87]})
        explorer_obj = explorer.Dataset_info_provider(test_df)

        unique_col_A_values = explorer_obj.get_unique_attribute_values('col_A')
        self.assertEqual(unique_col_A_values.all(),np.array([1, 2, 7]).all())
        
