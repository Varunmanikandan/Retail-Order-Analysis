import streamlit as st
import mysql.connector
import pandas as pd

# ---- Page Configuration ----
st.set_page_config(page_title="Retail Order Data Analysis", layout="wide")

# ---- Initialize Session State ----
if "main_page" not in st.session_state:
    st.session_state.main_page = "FIRST 10 QUERIES"

def set_main_page(page_name):
    st.session_state.main_page = page_name

# ---- Title and Navigation ----
st.title("🌐 Retail Order Data Analysis")

col1, col2 = st.columns(2)
with col1:
    if st.button("📊 FIRST 10 QUERIES"):
        set_main_page("FIRST 10 QUERIES")
with col2:
    if st.button("📦 LAST 10 QUERIES"):
        set_main_page("LAST 10 QUERIES")

st.divider()

# ---- Database Configuration ----
DB_CONFIG = {
    "host": "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
    "port": 4000,
    "user": "2rrW5N2MLfbGdMz.root",
    "password": "P4bsQKQohFuSXOLc",
    "database": "PRO1"
}

def connect_db():
    """Establish a database connection."""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except mysql.connector.Error as e:
        st.sidebar.error(f"❌ Database Connection Failed: {e}")
        return None

def execute_query(query):
    """Execute a SQL query and return the result as a DataFrame."""
    connection = connect_db()
    if connection is None:
        return pd.DataFrame()
    
    try:
        cursor = connection.cursor(buffered=True)
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    except mysql.connector.Error as e:
        st.error(f"⚠ Error executing query: {e}")
        return pd.DataFrame()
    finally:
        cursor.close()
        connection.close()

connection = connect_db()
if connection:
    st.sidebar.success("✅ Connected to Database")
else:
    st.sidebar.error("❌ Database Connection Failed")

st.sidebar.markdown("📌 **Session Info:**")
st.sidebar.text(f"Current Page: {st.session_state.main_page}")
st.sidebar.divider()

# ---- First 10 Queries ----
queries_dict = {
    "1. Top 10 highest revenue products": """SELECT Sub_Category, SUM(Sale_Price * Quantity) AS Total_Revenue 
                                             FROM Order2 GROUP BY Sub_Category 
                                             ORDER BY Total_Revenue DESC LIMIT 10""",
    "2. Top 5 cities with highest profit margins": """SELECT o1.City, SUM(o2.Profit) AS Total_Profit, 
                                              SUM(o2.Sale_Price * o2.Quantity) AS Total_Revenue, 
                                              (SUM(o2.Profit) / SUM(o2.Sale_Price * o2.Quantity)) * 100 AS Profit_Margin 
                                              FROM Order1 o1 JOIN Order2 o2 ON o1.Order_Id = o2.Order_Id 
                                              GROUP BY o1.City ORDER BY Profit_Margin DESC LIMIT 5""",
    "3. Total discount given per category": "SELECT Category, SUM(Discount_Value) AS Total_Discount FROM Order2 GROUP BY Category",
    "4. Average sale price per category": "SELECT Sub_Category, AVG(Sale_Price) AS Average_Sale_Price FROM Order2 GROUP BY Sub_Category ORDER BY Average_Sale_Price DESC",
    "5. Region with highest average sale price": """SELECT o1.Region, AVG(o2.Sale_Price) AS Avg_Sale_Price 
                                                   FROM Order1 o1 JOIN Order2 o2 ON o1.Order_Id = o2.Order_Id 
                                                   GROUP BY o1.Region ORDER BY Avg_Sale_Price DESC LIMIT 1""",
    "6. Total profit per category": "SELECT Category, SUM(Profit) AS Total_Profit FROM Order2 GROUP BY Category ORDER BY Total_Profit DESC",
    "7. Top 3 segments with highest quantity of orders": """SELECT o1.Segment, SUM(o2.Quantity) AS Total_Orders 
                                                           FROM Order1 o1 JOIN Order2 o2 ON o1.Order_Id = o2.Order_Id 
                                                           GROUP BY o1.Segment ORDER BY Total_Orders DESC LIMIT 3""",
    "8. Average discount percentage per region": "SELECT o1.Region, AVG(o2.Discount_Percent) AS Avg_Discount_Percent FROM Order1 o1 JOIN Order2 o2 ON o1.Order_Id = o2.Order_Id GROUP BY o1.Region",
    "9. Product category with highest total profit": "SELECT Sub_Category, SUM(Profit) AS Total_Profit FROM Order2 GROUP BY Sub_Category ORDER BY Total_Profit DESC LIMIT 1",
    "10. Total revenue generated per year": """SELECT YEAR(o1.Order_Date) AS Year, SUM(o2.Sale_Price * o2.Quantity) AS Total_Revenue 
                                               FROM Order2 o2 JOIN Order1 o1 ON o1.Order_Id = o2.Order_Id 
                                               GROUP BY YEAR(o1.Order_Date) ORDER BY Year DESC"""
}
# ---- Last 10 Queries ----
last_10_queries_dict = {
    "1. Total revenue for each city": """SELECT o.City, SUM(o2.Sale_Price * o2.Quantity) AS Total_Revenue
                                       FROM Order1 o JOIN Order2 o2 ON o.Order_Id = o2.Order_Id
                                       GROUP BY o.City ORDER BY Total_Revenue DESC""",
    "2. Average profit margin per region": """SELECT o.Region, AVG(o2.Profit / o2.Sale_Price) * 100 AS Avg_Profit_Margin 
                                              FROM Order1 o JOIN Order2 o2 ON o.Order_Id = o2.Order_Id 
                                              GROUP BY o.Region ORDER BY Avg_Profit_Margin DESC""",
    "3. Top 5 most discounted products": """SELECT Product_Id, Sub_Category, Category, AVG(Discount_Percent) AS Avg_Discount
                                          FROM Order2 GROUP BY Product_Id, Sub_Category, Category
                                          ORDER BY Avg_Discount DESC LIMIT 5""",
    "4. Total revenue per segment": """SELECT o.Segment, SUM(o2.Sale_Price * o2.Quantity) AS Total_Revenue
                                      FROM Order1 o JOIN Order2 o2 ON o.Order_Id = o2.Order_Id
                                      GROUP BY o.Segment ORDER BY Total_Revenue DESC""",
    "5. Top-selling products by quantity": """SELECT Product_Id, Category, SUM(Quantity) AS Total_Quantity_Sold
                                            FROM Order2 GROUP BY Product_Id, Category ORDER BY Total_Quantity_Sold DESC LIMIT 10""",
    "6. Most popular ship mode by total sales": """SELECT o.ship_mode, SUM(od.sale_price * od.quantity) AS total_sales
                                                   FROM order1 o JOIN order2 od ON o.order_id = od.order_id
                                                   GROUP BY o.ship_mode ORDER BY total_sales DESC""",
    "7. City with highest total discount given": """SELECT o.city, SUM(od.discount_value) AS total_discount
                                                    FROM order1 o JOIN order2 od ON o.order_id = od.order_id
                                                    GROUP BY o.city ORDER BY total_discount DESC LIMIT 1""",
    "8. Total number of orders per state": """SELECT o.state, COUNT(o.order_id) AS total_orders
                                              FROM order1 o GROUP BY o.state ORDER BY total_orders DESC""",
    "9. Most frequently ordered product per region": """SELECT o.region, od.product_id, COUNT(od.product_id) AS order_count
                                                        FROM order1 o JOIN order2 od ON o.order_id = od.order_id
                                                        GROUP BY o.region, od.product_id ORDER BY order_count DESC""",
    "10. Total revenue generated for each postal code": """SELECT o.postal_code, SUM(od.sale_price * od.quantity) AS total_revenue
                                                           FROM order1 o JOIN order2 od ON o.order_id = od.order_id 
                                                           GROUP BY o.postal_code ORDER BY total_revenue DESC"""
}

query_dict = queries_dict if st.session_state.main_page == "FIRST 10 QUERIES" else last_10_queries_dict

selected_query = st.selectbox("🔍 Select a query:", list(query_dict.keys()))

if st.button("🚀 Run Query"):
    df_result = execute_query(query_dict[selected_query])
    
    if not df_result.empty:
        st.dataframe(df_result)
        csv = df_result.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download CSV", csv, f"{selected_query}.csv", "text/csv")
    else:
        st.warning("⚠ No data found or query execution failed.")
