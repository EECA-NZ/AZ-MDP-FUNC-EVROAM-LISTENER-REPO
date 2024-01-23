"""Test module for the Azure HTTP trigger function evroam_listener."""

import unittest
from unittest.mock import Mock, patch

# The evroam_listener module needs to be in the Python path.
# If you encounter an import error, ensure that your PYTHONPATH is set correctly,
# or modify the import statement to reflect the structure of your project.
from evroam_listener import main


class TestHttpTrigger(unittest.TestCase):
    """Unit tests for the HTTP trigger function."""

    @patch('evroam_listener.logging')
    def test_main_name_provided(self, _):
        """Test the main function when a name is provided."""
        # Arrange
        req = Mock()
        req.params = {'name': 'Azure'}
        req.get_json = Mock(side_effect=Exception('No JSON provided'))

        # Act
        resp = main(req)

        # Assert
        self.assertEqual(
            resp.get_body(),
            b"Hello, Azure. This HTTP triggered function executed successfully.",
            "Response body does not match expected greeting."
        )
        self.assertEqual(resp.status_code, 200)

    @patch('evroam_listener.logging')
    def test_main_name_not_provided(self, _):
        """Test the main function when no name is provided."""
        # Arrange
        req = Mock()
        req.params = {}
        req.get_json = Mock(return_value={})

        # Act
        resp = main(req)

        # Assert
        self.assertIn(
            b"Pass a name in the query string or in the request body",
            resp.get_body(),
            "Response body for missing name is not as expected."
        )
        self.assertEqual(resp.status_code, 200)


if __name__ == '__main__':
    unittest.main()
