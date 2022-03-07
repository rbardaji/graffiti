import unittest
import urllib3

from run import app


class ResourceTest(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()

    def test_get_token_response_401(self):
        """
        GET /user/token/<user>/<password> with an invalid user and password.
        The status_code of the response should be 401.
        """
        query = '/user/token/bad_user/bad_password'
        response = self.app.get(query)
        self.assertEqual(401, response.status_code)
