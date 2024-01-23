"""Test module for evroam_aggregator Azure function.

This module contains unittests for the timer trigger Azure function.
"""

import unittest
from unittest.mock import Mock, patch, ANY as ANY_CONST

# Assuming 'main' is the correct entry point for your function and
# 'evroam_listener' is the correct import path. Adjust if necessary.
from evroam_aggregator import main


class TestKeteAggregatorFunction(unittest.TestCase):
    """Tests for the evroam_listener function."""

    @patch('evroam_aggregator.logging')
    def test_timer_trigger(self, mock_logging):
        """Test the timer trigger Azure function.

        It ensures that the function logs the correct messages based on
        whether the timer is past due or not.
        """
        # Test when the timer is not past due
        mock_timer_request = Mock()
        mock_timer_request.past_due = False
        main(mock_timer_request)
        mock_logging.info.assert_called_with(
            'Python timer trigger function ran at %s', ANY_CONST
        )

        # Reset the mock to clear the previous call history
        mock_logging.reset_mock()

        # Test when the timer is past due
        mock_timer_request.past_due = True
        main(mock_timer_request)
        # Check if the specific past due log was made
        mock_logging.info.assert_any_call('The timer is past due!')


if __name__ == '__main__':
    unittest.main()
