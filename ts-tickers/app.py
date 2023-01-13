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


# --- Create and run queries, displaying as desired

"""
Most traded symbols in the last 14 days
"""
bucket = '14 day'
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
Apple's trading volume over time
"""
symbol = 'AAPL'
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


"""
Apple's stock price over time
"""
bucket = '7 day'
symbol = 'AAPL'
query = f"""
    SELECT time_bucket('{bucket}', time) AS bucket,
    last(price_close, time) AS last_closing_price
    FROM stocks_intraday
    WHERE symbol = '{symbol}'
    GROUP BY bucket
    ORDER BY bucket
"""
df = load_data(query)
df
st.line_chart(df[["last_closing_price"]])


"""
Top weekly gainers over time
"""
bucket = '7 days'
orderby = 'DESC'
query = f"""
    SELECT symbol, bucket, max((closing_price-opening_price)/closing_price*100) AS price_change_pct
    FROM (
        SELECT
        symbol,
        time_bucket('{bucket}', time) AS bucket,
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
bucket = '7 day'
symbols = "('META', 'AAPL', 'AMZN', 'NFLX', 'GOOG')"
query = f"""
    SELECT symbol, time_bucket('{bucket}', time) AS bucket,
    last(price_close, time) AS last_closing_price
    FROM stocks_intraday
    WHERE symbol in {symbols}
    GROUP BY bucket, symbol
    ORDER BY bucket
"""
df = load_data(query)
df = df.pivot(index="bucket", columns="symbol", values="last_closing_price")
st.line_chart(df)