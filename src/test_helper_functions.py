import pytest
import pandas as pd
import numpy as np

from helper_functions import get_etf_mbs_change, get_treasury_change

@pytest.fixture
def etf_mbs_data():
    """Fixture for ETF MBS data"""
    return pd.DataFrame({
        'Date': ["03/31/2022", "03/31/2023"],
        'Close/Last': [100, 110]  # 10% increase
    })

@pytest.fixture
def treasury_data():
    """Fixture for Treasury index data"""
    return pd.DataFrame({
        'date': ["2022-03-31", "2023-03-31"],
        'index': [200, 180]  # 10% decrease
    })

def test_get_etf_mbs_change_valid(etf_mbs_data):
    """Test valid ETF MBS data"""
    result = get_etf_mbs_change(etf_mbs_data)
    expected = 0.10  # (110/100) - 1 = 10%
    assert np.isclose(result, expected), f"Expected {expected}, got {result}"

def test_get_treasury_change_valid(treasury_data):
    """Test valid Treasury data"""
    result = get_treasury_change(treasury_data)
    expected = -0.10  # (180/200) - 1 = -10%
    assert np.isclose(result, expected), f"Expected {expected}, got {result}"

def test_get_etf_mbs_change_missing_date(etf_mbs_data):
    """Test ETF MBS change with a missing date"""
    etf_mbs_data = etf_mbs_data[etf_mbs_data['Date'] != "03/31/2022"]
    with pytest.raises(IndexError):
        get_etf_mbs_change(etf_mbs_data)

def test_get_treasury_change_missing_date(treasury_data):
    """Test Treasury change with a missing date"""
    treasury_data = treasury_data[treasury_data['date'] != "2022-03-31"]
    with pytest.raises(IndexError):
        get_treasury_change(treasury_data)

def test_get_etf_mbs_change_empty():
    """Test ETF MBS change with an empty DataFrame"""
    df = pd.DataFrame(columns=['Date', 'Close/Last'])
    with pytest.raises(IndexError):
        get_etf_mbs_change(df)

def test_get_treasury_change_empty():
    """Test Treasury change with an empty DataFrame"""
    df = pd.DataFrame(columns=['date', 'index'])
    with pytest.raises(IndexError):
        get_treasury_change(df)

def test_get_etf_mbs_change_non_numeric():
    """Test ETF MBS change with non-numeric values"""
    df = pd.DataFrame({
        'Date': ["03/31/2022", "03/31/2023"],
        'Close/Last': ["A", "B"]
    })
    with pytest.raises(TypeError):
        get_etf_mbs_change(df)

def test_get_treasury_change_non_numeric():
    """Test Treasury change with non-numeric values"""
    df = pd.DataFrame({
        'date': ["2022-03-31", "2023-03-31"],
        'index': ["X", "Y"]
    })
    with pytest.raises(TypeError):
        get_treasury_change(df)
