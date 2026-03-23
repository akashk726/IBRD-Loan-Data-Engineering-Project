import streamlit as st
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv
import plotly.express as px

# =========================
# CONFIG
# =========================
load_dotenv()
st.set_page_config(page_title="IBRD Dashboard", layout="wide")

# =========================
# LOAD DATA
# =========================
@st.cache_data(ttl=600)
def load_data(query):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="IBRD_WH",
        database="IBRD_DB",
        schema="DW"
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df

fact = load_data("SELECT * FROM FACT_LOANS")
country = load_data("SELECT * FROM DIM_COUNTRY")
year = load_data("SELECT * FROM DIM_YEAR")
borrower = load_data("SELECT * FROM DIM_BORROWER")
loan_type = load_data("SELECT * FROM DIM_LOAN_TYPE")
status = load_data("SELECT * FROM DIM_STATUS")

# =========================
# CLEAN + MERGE
# =========================
for d in [fact, country, year, borrower, loan_type, status]:
    d.columns = d.columns.str.lower()

df = fact.merge(country, on="country_key", how="left")
df = df.merge(year, on="year_key", how="left")
df = df.merge(borrower, on="borrower_key", how="left")
df = df.merge(loan_type, on="loan_type_key", how="left")

if "status_key" in df.columns:
    df = df.merge(status, on="status_key", how="left")

df.columns = df.columns.str.lower()

# =========================
# FIX COLUMNS
# =========================
for col in ["country", "country_x", "country_y", "country_name"]:
    if col in df.columns:
        df.rename(columns={col: "country"}, inplace=True)
        break

for col in ["year", "loan_year", "year_key"]:
    if col in df.columns:
        df.rename(columns={col: "year"}, inplace=True)
        break

# YEAR CLEAN
df["year"] = pd.to_numeric(df["year"], errors="coerce")
current_year = pd.Timestamp.now().year
df = df.dropna(subset=["year"])
df = df[(df["year"] >= 1900) & (df["year"] <= current_year)]
df["year"] = df["year"].astype(int)

# STATUS FIX
for col in ["loan_status","loan_status_x","loan_status_y","status","status_name"]:
    if col in df.columns:
        df.rename(columns={col: "loan_status"}, inplace=True)
        break

if "loan_status" not in df.columns:
    df["loan_status"] = "unknown"

df["loan_status"] = df["loan_status"].astype(str).str.lower()

# CLASSIFICATION
def classify_status(s):
    if "cancel" in s:
        return "Cancelled"
    elif any(x in s for x in ["disburs","active","approved"]):
        return "Active"
    elif any(x in s for x in ["closed","fully","complete"]):
        return "Closed"
    else:
        return "Other"

df["status_group"] = df["loan_status"].apply(classify_status)

# NUMERIC CLEAN
for col in ["disbursed_amount","original_principal_amount","repayment_ratio"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# =========================
# SIDEBAR FILTERS
# =========================
st.sidebar.title("🔎 Filters")

regions = sorted(df["region"].dropna().unique()) if "region" in df.columns else []
sel_r = st.sidebar.multiselect("Region", regions)
df = df[df["region"].isin(sel_r)] if sel_r else df

countries = sorted(df["country"].dropna().unique())
sel_c = st.sidebar.multiselect("Country", countries)
df = df[df["country"].isin(sel_c)] if sel_c else df

years = sorted(df["year"].unique())
sel_y = st.sidebar.multiselect("Year", years, default=years[-5:])
df = df[df["year"].isin(sel_y)] if sel_y else df

# 🔥 TOP N FILTER
top_n = st.sidebar.selectbox("📊 Top N", [10, 20, 50, 100], index=0)

if df.empty:
    st.warning("⚠️ No data available")
    st.stop()

# =========================
# TITLE
# =========================
st.markdown("""
<h2 style='text-align:center; color:white;'>
🌍 IBRD Loan Analytics Dashboard
</h2>
""", unsafe_allow_html=True)

# =========================
# TABS
# =========================
tab1, tab2, tab3 = st.tabs(["📊 Overview","📈 Trends","🏢 Details"])

# =========================
# TAB 1: OVERVIEW
# =========================
with tab1:

    st.markdown("### 📊 Key Performance Indicators")

    total = len(df)
    cancelled = len(df[df["status_group"]=="Cancelled"])
    active = len(df[df["status_group"]=="Active"])

    cancel_rate = cancelled/total*100 if total else 0
    approval_rate = active/total*100 if total else 0

    col1,col2,col3,col4 = st.columns(4)
    col1.metric("Total Loans", total)
    col2.metric("Disbursed", f"${df['disbursed_amount'].sum():,.0f}")
    col3.metric("Principal", f"${df['original_principal_amount'].sum():,.0f}")
    col4.metric("Avg Repayment", f"{df['repayment_ratio'].mean():.2f}")

    col5,col6,col7,col8 = st.columns(4)
    col5.metric("Cancelled", cancelled)
    col6.metric("Active", active)
    col7.metric("Cancel %", f"{cancel_rate:.1f}%")
    col8.metric("Approval %", f"{approval_rate:.1f}%")

    st.markdown("---")

    # Loan Status
    st.markdown("### 📊 Loan Status Distribution")
    status_data = df["status_group"].value_counts().reset_index()
    status_data.columns = ["status","count"]

    fig1 = px.pie(status_data, names="status", values="count")
    fig1.update_layout(height=300)
    st.plotly_chart(fig1, use_container_width=True)

    # Country
    st.markdown("### 🌍 Country Disbursement")

    country_data = df.groupby("country")["disbursed_amount"].sum().reset_index()
    country_data = country_data.sort_values(by="disbursed_amount", ascending=False).head(top_n)

    fig2 = px.bar(
        country_data,
        x="country",
        y="disbursed_amount",
        title=f"Top {top_n} Countries"
    )
    fig2.update_layout(height=300)

    st.plotly_chart(fig2, use_container_width=True)

# =========================
# TAB 2: TRENDS
# =========================
with tab2:

    st.markdown("### 📈 Loan Trends")

    colC,colD = st.columns(2)

    year_data = df.groupby("year")["disbursed_amount"].sum().reset_index()
    fig3 = px.line(year_data, x="year", y="disbursed_amount")
    fig3.update_layout(height=350)

    cancel_trend = df.groupby("year").apply(
        lambda x: len(x[x["status_group"]=="Cancelled"]) / len(x) * 100
    ).reset_index(name="rate")

    fig4 = px.line(cancel_trend, x="year", y="rate")
    fig4.update_layout(height=350)

    colC.plotly_chart(fig3, use_container_width=True)
    colD.plotly_chart(fig4, use_container_width=True)

    st.markdown("### 📊 Approval Trend")

    approval_trend = df.groupby("year").apply(
        lambda x: len(x[x["status_group"]=="Active"]) / len(x) * 100
    ).reset_index(name="rate")

    fig5 = px.line(approval_trend, x="year", y="rate")
    fig5.update_layout(height=350)

    st.plotly_chart(fig5, use_container_width=True)

# =========================
# TAB 3: DETAILS
# =========================
with tab3:

    st.markdown("### 🏢 Borrower Analysis")

    borrower_data = df.groupby("borrower")["original_principal_amount"].sum().reset_index()
    borrower_data = borrower_data.sort_values(by="original_principal_amount", ascending=False).head(top_n)

    fig6 = px.bar(
        borrower_data,
        x="original_principal_amount",
        y="borrower",
        orientation="h",
        title=f"Top {top_n} Borrowers"
    )

    fig6.update_layout(height=500)

    st.plotly_chart(fig6, use_container_width=True)

    # TOP CANCELLED COUNTRIES
    st.markdown("### ❌ Top Cancellation Countries")

    cancel_country = df[df["status_group"]=="Cancelled"] \
        .groupby("country").size().reset_index(name="count") \
        .sort_values(by="count", ascending=False).head(top_n)

    fig_cancel = px.bar(cancel_country, x="country", y="count")
    st.plotly_chart(fig_cancel, use_container_width=True)

    st.markdown("### 📋 Data Preview")
    st.dataframe(df.head(20))