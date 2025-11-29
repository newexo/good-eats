import pandas as pd
import pytest

from good_eats.directories import test_data


# ---------------------------------------------------------
# Pytest Fixture
# ---------------------------------------------------------
@pytest.fixture
def fake_df():
    """
    Load the synthetic Good Eats dataset from the test data CSV file.
    Returns a pandas DataFrame for use in test functions.
    """
    path = test_data("fake_good_eats.csv")
    df = pd.read_csv(path)
    return df


# ---------------------------------------------------------
# Schema Tests
# ---------------------------------------------------------
def test_columns_present(fake_df):
    """
    Verify that the required schema is present and in the correct order.
    """
    expected_columns = [
        "Agency",
        "Time Period",
        "Food Product Group",
        "Food Product Category",
        "Product Name",
        "Product Type",
        "Origin Detail",
        "Distributor",
        "Vendor",
        "# of Units",
        "Total Weight in lbs",
        "Total Cost",
    ]
    assert list(fake_df.columns) == expected_columns


def test_no_missing_except_health_hospitals(fake_df):
    """
    All non-Health + Hospitals rows must have non-null Total Cost.
    Health + Hospitals is allowed to have Total Cost = NaN.
    """
    non_hh = fake_df[fake_df["Agency"] != "Health + Hospitals"]
    assert non_hh["Total Cost"].notna().all()


# ---------------------------------------------------------
# Categorical Domain Tests
# ---------------------------------------------------------
def test_agency_domain(fake_df):
    """
    The dataset should only contain the selected three agencies.
    """
    allowed_agencies = {
        "Administration for Childrens Services",
        "Health + Hospitals",
        "Department of Education",
    }
    assert set(fake_df["Agency"].unique()).issubset(allowed_agencies)
    assert len(fake_df["Agency"].unique()) >= 2  # variance check


def test_time_period_domain(fake_df):
    """
    Time period values must be drawn from the approved list.
    """
    allowed_periods = {"2018-2019", "2019-2020", "2020-2021"}
    assert set(fake_df["Time Period"].unique()).issubset(allowed_periods)
    assert len(fake_df["Time Period"].unique()) >= 2  # variance check


# ---------------------------------------------------------
# Quantitative Tests
# ---------------------------------------------------------
def test_units_weight_cost_positive(fake_df):
    """
    # of Units and Total Weight must be positive for all rows.
    Total Cost must be positive for all rows EXCEPT Health + Hospitals,
    which legitimately has missing values in this field.
    """
    # Tests for fields that must always be positive
    assert (fake_df["# of Units"] > 0).all()
    assert (fake_df["Total Weight in lbs"] > 0).all()

    # Split into groups
    hh = fake_df[fake_df["Agency"] == "Health + Hospitals"]
    non_hh = fake_df[fake_df["Agency"] != "Health + Hospitals"]

    # Health + Hospitals always has NaN Total Cost
    assert hh["Total Cost"].isna().all()

    # All other agencies must have strictly positive Total Cost
    assert (non_hh["Total Cost"] > 0).all()


def test_cost_correlated_with_weight(fake_df):
    """
    A loose sanity check: higher total weight should generally correspond
    to higher cost for most rows. We do not require strict ordering,
    only that correlation is positive.
    """
    corr = fake_df["Total Weight in lbs"].corr(fake_df["Total Cost"])
    assert corr > 0.3  # modest positive correlation expected in synthetic data


# ---------------------------------------------------------
# Variance / Diversity Tests
# ---------------------------------------------------------
def test_multiple_product_groups(fake_df):
    """
    Confirm that there is meaningful variance across product groups.
    """
    assert fake_df["Food Product Group"].nunique() >= 3


def test_multiple_distributors(fake_df):
    """
    Since synthetic data uses multiple distributors, verify diversity.
    """
    assert fake_df["Distributor"].nunique() >= 2


# ---------------------------------------------------------
# Aggregate Tests (Sums and Means)
# ---------------------------------------------------------


def test_total_units_sum(fake_df):
    """
    Test the total sum of # of Units across the entire dataset.
    The expected value should be filled in after computing it manually
    or from a trusted pandas run.
    """
    total_units = fake_df["# of Units"].sum()
    expected = 7205
    assert total_units == expected


def test_total_weight_sum(fake_df):
    """
    Test the total sum of Total Weight in lbs across the dataset.
    """
    total_weight = fake_df["Total Weight in lbs"].sum()
    expected = 21475
    assert total_weight == expected


def test_total_cost_sum_non_hh(fake_df):
    """
    Test the total sum of Total Cost, excluding Health + Hospitals.
    Health + Hospitals has NaN cost, so we drop those rows.
    """
    non_hh = fake_df[fake_df["Agency"] != "Health + Hospitals"]
    total_cost = non_hh["Total Cost"].sum()
    expected = 36950
    assert total_cost == expected


def test_mean_units(fake_df):
    """
    Test the mean of # of Units across the dataset.
    """
    mean_units = fake_df["# of Units"].mean()
    expected = 720.5
    assert mean_units == expected


def test_mean_weight(fake_df):
    """
    Test the mean of Total Weight in lbs across the dataset.
    """
    mean_weight = fake_df["Total Weight in lbs"].mean()
    expected = 2147.5
    assert mean_weight == expected


def test_mean_cost_non_hh(fake_df):
    """
    Test the mean of Total Cost excluding Health + Hospitals rows (which have NaN).
    """
    non_hh = fake_df[fake_df["Agency"] != "Health + Hospitals"]
    mean_cost = non_hh["Total Cost"].mean()
    expected = 6158.333333333333
    assert mean_cost == expected
