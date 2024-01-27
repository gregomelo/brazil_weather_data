import pytest

from app.tools.validators import validate_sublists


@pytest.fixture()
def list_with_equal_elements_equal_order():
    """Provide a list where sublists have identical elements in the same order.

    Returns
    -------
    list
        A list of sublists with equal elements in identical order.
    """
    return [
        ["A", "B"],
        ["A", "B"],
    ]


@pytest.fixture()
def list_with_equal_elements_different_order():
    """Provide a list where sublists have identical elements in different
    orders.

    Returns
    -------
    list
        A list of sublists with equal elements in varying order.
    """
    return [
        ["A", "B"],
        ["B", "A"],
    ]


@pytest.fixture()
def list_with_unequal_elements():
    """Provide a list where sublists have different elements.

    Returns
    -------
    list
        A list of sublists with unequal elements.
    """
    return [
        ["A", "B"],
        ["C", "A"],
    ]


@pytest.fixture()
def list_with_unequal_elements_and_unbalance():
    """Provide a list where sublists have different elements and lengths.

    Returns
    -------
    list
        A list of sublists with unequal elements and varying lengths.
    """
    return [
        ["A", "B", "C"],
        ["C", "A"],
    ]


class TestValidateSublists:
    def test_equal_elements_and_order(
        self,
        list_with_equal_elements_equal_order,
    ):
        """Verify validate_sublists function with lists having identical
        elements in the same order.

        Parameters
        ----------
        list_with_equal_elements_equal_order : list
            Input provided by a fixture.

        Asserts
        -------
        The function returns True for lists with identical elements and order.
        """
        assert validate_sublists(list_with_equal_elements_equal_order)

    def test_equal_elements_and_different_order(
        self,
        list_with_equal_elements_different_order,
    ):
        """Verify validate_sublists function with lists having identical
        elements in different orders.

        Parameters
        ----------
        list_with_equal_elements_different_order : list
            Input provided by a fixture.

        Asserts
        -------
        The function returns True for lists with identical elements
        regardless of their order.
        """
        assert validate_sublists(list_with_equal_elements_different_order)

    def test_unequal_elements(self, list_with_unequal_elements):
        """Verify validate_sublists function with lists having different
        elements.

        Parameters
        ----------
        list_with_unequal_elements : list
            Input provided by a fixture.

        Asserts
        -------
        The function raises a ValueError for lists with differing elements.
        """
        with pytest.raises(ValueError) as excinfo:
            validate_sublists(list_with_unequal_elements)
        assert "Sublists do not have the same elements." in str(excinfo.value)

    def test_unequal_elements_and_unbalance(
        self,
        list_with_unequal_elements_and_unbalance,
    ):
        """Verify validate_sublists function with lists having different
        elements and lengths.

        Parameters
        ----------
        list_with_unequal_elements_and_unbalance : list
            Input provided by a fixture.

        Asserts
        -------
        The function raises a ValueError for lists with differing elements
        and lengths.
        """
        with pytest.raises(ValueError) as excinfo:
            validate_sublists(list_with_unequal_elements_and_unbalance)
        assert "Sublists do not have the same elements." in str(excinfo.value)
