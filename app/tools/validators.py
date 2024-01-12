"""Data Validators.

This module provides utility functions for data validation, such as
asserting that all sublists within a list have the same elements, regardless
of their order. This can be particularly useful for validating if a list of
dataframes have consistent column names across all dataframes.

"""

from typing import List


def validate_sublists(list_with_sublists: List[List[str]]) -> bool:
    """Validate whether all sublists within a list contain the same elements.

    This function is useful for validating if a list of dataframes has
    consistent column names. It checks if every sublist (representing
    dataframe columns) contains the same set of elements (column names).

    Parameters
    ----------
    list_with_sublists : List[List[str]]
        List containing sublists to be validated.

    Returns
    -------
    bool
        Returns True if all sublists have the same elements, otherwise raises
        an exception.

    Raises
    ------
    ValueError
        If any of the sublists differ in elements.

    Examples
    --------
    >>> validate_sublists([["A", "B"], ["B", "A"]])
    True
    >>> validate_sublists([["C", "B"], ["B", "A"]])
    ValueError: Sublists do not have the same elements.
    """
    set_columns = set(tuple(sorted(sublist)) for sublist in list_with_sublists)
    if len(set_columns) == 1:
        return True
    else:
        raise ValueError("Sublists do not have the same elements.")


if __name__ == "__main__":
    LIST_COLUMNS_VALID = [["A", "B"], ["B", "A"]]
    LIST_COLUMNS_INVALID = [["C", "B"], ["B", "A"]]

    try:
        print(
            "List with same elements:",
            validate_sublists(LIST_COLUMNS_VALID),
        )
    except ValueError as e:
        print("Error:", e)

    try:
        print(
            "List with different elements:",
            validate_sublists(LIST_COLUMNS_INVALID),
        )
    except ValueError as e:
        print("Error:", e)
