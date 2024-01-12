import pytest

from app.tools.validators import validate_sublists


@pytest.fixture()
def list_with_equal_elements_equal_order():
    """Create a list with the same elements in the same order."""
    return [
        ["A", "B"],
        ["A", "B"],
    ]


@pytest.fixture()
def list_with_equal_elements_different_order():
    """Create a list with the same elements but in different order."""
    return [
        ["A", "B"],
        ["B", "A"],
    ]


@pytest.fixture()
def list_with_unequal_elements():
    """Create a list with different elements."""
    return [
        ["A", "B"],
        ["C", "A"],
    ]


@pytest.fixture()
def list_with_unequal_elements_and_unbalance():
    """Create a list with unequal elements and varying lengths."""
    return [
        ["A", "B", "C"],
        ["C", "A"],
    ]


class TestValidateSublists:
    def test_equal_elements_and_order(
        self,
        list_with_equal_elements_equal_order,
    ):
        """
        Test validate_sublists function with lists having the same elements
        in the same order.

        Asserts that the function returns True for lists with identical
        elements and order.

        Parameters:
        list_with_equal_elements_equal_order (list): The input provided by
        a fixture.
        """
        assert validate_sublists(self, list_with_equal_elements_equal_order)

    def test_equal_elements_and_different_order(
        self,
        list_with_equal_elements_different_order,
    ):
        """
        Test validate_sublists function with lists having the same elements in
        different order.

        Asserts that the function returns True for lists with identical
        elements regardless of their order.

        Parameters:
        list_with_equal_elements_different_order (list): The input provided by
        a fixture.
        """
        assert validate_sublists(list_with_equal_elements_different_order)

    def test_unequal_elements(self, list_with_unequal_elements):
        """
        Test validate_sublists function with lists having different elements.

        Asserts that the function raises a ValueError for lists with differing
        elements.

        Parameters:
        list_with_unequal_elements (list): The input provided by a fixture.
        """
        with pytest.raises(ValueError) as excinfo:
            validate_sublists(list_with_unequal_elements)
        assert "Sublists do not have the same elements." in str(excinfo.value)

    def test_unequal_elements_and_unbalance(
        self,
        list_with_unequal_elements_and_unbalance,
    ):
        """
        Test validate_sublists function with lists having different elements
        and varying lengths.

        Asserts that the function raises a ValueError for lists with differing
        elements and lengths.

        Parameters:
        list_with_unequal_elements_and_unbalance (list): The input provided by
        a fixture.
        """
        with pytest.raises(ValueError) as excinfo:
            validate_sublists(list_with_unequal_elements_and_unbalance)
        assert "Sublists do not have the same elements." in str(excinfo.value)
