import os
import tempfile
import unittest
from csv_parser import CSVParser


class TestCSVParser(unittest.TestCase):

    def setUp(self):
        """
        Runs before each test case
        """
        self.required_fields = ["transaction_id", "amount"]
        self.parser = CSVParser(required_fields=self.required_fields)

    def _create_temp_csv(self, content: str) -> str:
        """
        Utility function to create a temporary CSV file
        """
        temp_file = tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".csv")
        temp_file.write(content)
        temp_file.close()
        return temp_file.name

    def test_valid_csv_parsing(self):
        """
        Test parsing of a valid CSV file
        """
        csv_content = """transaction_id,amount,description
                         TXN001,1000,Payment
                         TXN002,2500,Refund"""

        file_path = self._create_temp_csv(csv_content)
        records = self.parser.parse(file_path)

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["transaction_id"], "TXN001")
        self.assertEqual(records[1]["amount"], "2500")

        os.remove(file_path)

    def test_missing_required_field(self):
        """
        Test CSV with missing required field
        """
        csv_content = """transaction_id,description
                         TXN001,Payment"""

        file_path = self._create_temp_csv(csv_content)
        records = self.parser.parse(file_path)

        self.assertEqual(len(records), 0)
        os.remove(file_path)

    def test_empty_file(self):
        """
        Test empty CSV file
        """
        csv_content = ""
        file_path = self._create_temp_csv(csv_content)

        records = self.parser.parse(file_path)
        self.assertEqual(len(records), 0)

        os.remove(file_path)

    def test_extra_columns_allowed(self):
        """
        Test CSV with additional columns beyond required schema
        """
        csv_content = """transaction_id,amount,extra_col
                         TXN001,500,EXTRA"""

        file_path = self._create_temp_csv(csv_content)
        records = self.parser.parse(file_path)

        self.assertEqual(len(records), 1)
        self.assertIn("extra_col", records[0])

        os.remove(file_path)

    def test_file_not_found(self):
        """
        Test behavior when file does not exist
        """
        records = self.parser.parse("non_existent_file.csv")
        self.assertEqual(len(records), 0)


if __name__ == "__main__":
    unittest.main()
