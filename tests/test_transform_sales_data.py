import unittest
import pandas as pd
import tempfile
import os
from scripts.transform_sales_data import clean_and_aggregate

class TestDataTransformation(unittest.TestCase):
    def test_clean_and_aggregate(self):
        input_file = tempfile.NamedTemporaryFile(delete=False)
        output_file = tempfile.NamedTemporaryFile(delete=False)

        # Sample test data
        df = pd.DataFrame({
            "product_line": ["Classic Cars", None, "trains", "VINTAGE CARS"],
            "sales": [100, "invalid", 200, 9999999]
        })
        df.to_csv(input_file.name, index=False)

        mock_kwargs = {'ti': type('TI', (), {'xcom_pull': lambda *args, **kwargs: input_file.name})}
        result_path = clean_and_aggregate(**mock_kwargs)

        result_df = pd.read_csv(result_path)
        self.assertTrue((result_df['sales'] >= 0).all())
        self.assertIn("Vintage Cars", result_df.values)
        self.assertEqual(len(result_df), 2)

        os.unlink(input_file.name)
        os.unlink(output_file.name)

if __name__ == '__main__':
    unittest.main()