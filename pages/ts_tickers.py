import streamlit as st
import pandas as pd
import sqlalchemy
from sqlalchemy import text

# --- Setup the database connection

url = f'postgresql+psycopg2://{st.secrets.DB_USER}:{st.secrets.DB_PASS}@{st.secrets.DB_HOST}:{st.secrets.DB_PORT}/{st.secrets.DB_NAME}'
engine = sqlalchemy.create_engine(url)


# --- Create a cached function for returning sql as dataframe

@st.cache
def load_data(sql):
    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(text(sql))
    df = pd.DataFrame(query.fetchall())
    return df

# --- Setup variables and page config

st.title("Timescale Tickers Demo App")

bucket_value = st.sidebar.number_input(label="Change bucket size", min_value=1, max_value=365, step=1, value=14)
bucket = f'{bucket_value} day'

symbol_query = """
    SELECT DISTINCT symbol
    FROM stocks_intraday
"""
symbol_list = load_data(symbol_query)
symbol = st.sidebar.selectbox(label="Select a symbol", options=symbol_list)


# --- Predefined charts for a given bucket size

f"""
Most traded symbols in the last {bucket_value} days
"""
query = f"""
    SELECT symbol, max(price_high) AS "{bucket} high", sum(trading_volume) AS volume
    FROM stocks_intraday
    WHERE time > now() - INTERVAL '{bucket}'
    GROUP BY symbol
    ORDER BY volume DESC
    LIMIT 20
    """
df = load_data(query)
df 

"""
Top weekly gainers over time
"""
orderby = 'DESC'
query = f"""
    SELECT symbol, bucket, max((closing_price-opening_price)/closing_price*100) AS price_change_pct
    FROM (
        SELECT
        symbol,
        time_bucket('7 day', time) AS bucket,
        first(price_open, time) AS opening_price,
        last(price_close, time) AS closing_price
        FROM stocks_intraday
        GROUP BY bucket, symbol
    ) s
    GROUP BY symbol, s.bucket
    ORDER BY price_change_pct DESC
    LIMIT 10
"""
df = load_data(query)
df 

"""
Weekly FAANG prices over time
"""
symbols = "('META', 'AAPL', 'AMZN', 'NFLX', 'GOOG')"
query = f"""
    SELECT symbol, time_bucket('{bucket}', time) AS "{bucket} bucket",
    last(price_close, time) AS last_closing_price
    FROM stocks_intraday
    WHERE symbol in {symbols}
    GROUP BY "{bucket} bucket", symbol
    ORDER BY "{bucket} bucket"
"""
df = load_data(query)
df = df.pivot(index=f"{bucket} bucket", columns="symbol", values="last_closing_price")
st.line_chart(df)


# --- Charts for a single symbol

st.markdown(f"## ${symbol} Charts")

f"""
{symbol}'s high and low closing price
"""
query = f"""
SELECT max(price_high) AS high, min(price_low) AS low
FROM stocks_intraday
WHERE symbol = '{symbol}'
"""
df = load_data(query)
df


f"""
{symbol}'s trading volume over time
"""

bucket = '1 day'
query = f"""
    SELECT time_bucket('{bucket}', time) AS bucket, sum(trading_volume) AS volume
    FROM stocks_intraday
    WHERE symbol = '{symbol}'
    GROUP BY bucket
    ORDER BY bucket
"""
df = load_data(query)
df
st.line_chart(df[["volume"]])

f"""
{symbol}'s stock price over time
"""
query = f"""
    SELECT time_bucket('{bucket}', time) AS "{bucket} bucket",
    last(price_close, time) AS last_closing_price
    FROM stocks_intraday
    WHERE symbol = '{symbol}'
    GROUP BY "{bucket} bucket"
    ORDER BY "{bucket} bucket"
"""
df = load_data(query)
df
st.line_chart(df[["last_closing_price"]])
