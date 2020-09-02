class ProcessDataset():
    def __init__(self):
        self.dataset_ = None
        self.supervised_format_dataset_ = None

    def check_if_nan(self, x):
        """Comprueba posibles valores ausentes

           Args:
               x ([number]): value
           Returns:
               boolean value    
        """
        try:
            import numpy as np 
            import math

            is_nan = math.isnan(x)             

            return is_nan
        except Exception as exc:
            #log error with logger
            print(exc)
            return exc   

    def impute_missing_value(self, dataframe, value_to_set):
        """Sustituye valores nulos del dataframe por el valor indicado

           Args:
               x ([number]): valor a setear
               dataframe ([dataframe]): dataframe con valores nulos 
           Returns:
               dataframe sin valores nulos
        """
        try:
            import pandas as pd

            dataframe=dataframe.applymap(lambda x: value_to_set if pd.isna(x) else x)
            return dataframe
            
        except Exception as exc:
            #log error with logger
            print(exc)
            return exc




class IteratorHelper():
    '''
    Ejemplo que, a partir de un dataframe original de 2 columnas, genera una lista de todas las posibles combinaciones 
    de dichas columnas; una vez obtenidas esas combinaciones, trabajamos con todas las posibles combinaciones de los 
    dos arrays (cliente y producto en este caso), en lugar de realizar un bucle 'for' anidado
    '''
    def __init__(self):
        import pandas as pd
        import itertools

        self

    def make2DCartesianProduct(self, array_x, array_y, x_name='x', y_name='y'):
        try:
            return pd.DataFrame.from_records(itertools.product(array_x.reshape(-1, ), array_y.reshape(-1, )), 
                                            columns=[x_name, y_name])
        except Exception as exc:
            #log error with logger
            print(exc)
            return exc

    def make4DCartesianProduct(self, array_x, array_y, array_z, x_name='x', 
                               y_name='y', z_name='z'):
        try:
            return pd.DataFrame.from_records(itertools.product(array_x.reshape(-1, ), array_y.reshape(-1, ), 
                                            array_z.reshape(-1, )), columns=[x_name, y_name, z_name])
        except Exception as exc:
            #log error with logger
            print(exc)
            return exc
