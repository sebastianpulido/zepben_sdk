import pandas as pd
import sqlite3
import unittest

class TestSQLQueries(unittest.TestCase):

    def setUp(self):
        # Load CSV into pandas DataFrame
        self.df = pd.read_csv('your_file.csv')

        # Connect to an in-memory SQLite database
        self.conn = sqlite3.connect(':memory:')
        self.df.to_sql('data', self.conn, index=False, if_exists='replace')

    def test_query(self):
        expected_count = 10
        query = 'select count(*) from data'
        result = pd.read_sql(query, self.conn)
        self.assertEqual(result.iloc[0, 0], expected_count)

    def test_query2(self):
        query = 'SELECT * FROM data WHERE another_column > 50'
        result = pd.read_sql(query, self.conn)
        self.assertTrue(len(result) > 0)  # Assert that the result is not empty

    def tearDown(self):
        # Close the database connection
        self.conn.close()

if __name__ == '__main__':
    unittest.main()