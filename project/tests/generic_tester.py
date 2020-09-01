import unittest
import pytest


class CodeTests(unittest.TestCase):
    import pandas as pd

    def test_array_length(self):
        test_array = [1, 2, 3]
        l = len(test_array)
        self.assertEqual(l, 3)

        