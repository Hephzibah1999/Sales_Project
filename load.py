import logging
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from database import Product, Customer, Order, OrderItem, SessionLocal

# Configure logging
logging.basicConfig(
    filename='data_refresh.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

def refresh_data(session: Session, csv_path: str):
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"CSV loaded with {len(df)} rows.")

        # Insert unique Products
        products = df[['Product ID', 'Product Name', 'Category', 'Unit Price']].drop_duplicates()
        for _, row in products.iterrows():
            if not session.get(Product, str(row['Product ID'])):
                product = Product(
                    product_id=str(row['Product ID']),
                    name=row['Product Name'],
                    category=row['Category'],
                    unit_price=row['Unit Price']
                )
                session.add(product)
        session.commit()
        logger.info("Products inserted.")

        # Insert unique Customers
        customers = df[['Customer ID', 'Customer Name', 'Customer Email', 'Customer Address']].drop_duplicates()
        for _, row in customers.iterrows():
            if not session.get(Customer, str(row['Customer ID'])):
                customer = Customer(
                    customer_id=str(row['Customer ID']),
                    name=row['Customer Name'],
                    email=row['Customer Email'],
                    address=row['Customer Address']
                )
                session.add(customer)
        session.commit()
        logger.info("Customers inserted.")

        # Insert unique Orders
        orders = df[['Order ID', 'Customer ID', 'Date of Sale', 'Payment Method', 'Region']].drop_duplicates()
        for _, row in orders.iterrows():
            if not session.get(Order, str(row['Order ID'])):
                order_date = datetime.strptime(row['Date of Sale'], '%Y-%m-%d').date()
                order = Order(
                    order_id=str(row['Order ID']),
                    customer_id=str(row['Customer ID']),
                    order_date=order_date,
                    payment_method=row['Payment Method'],
                    region=row['Region']
                )
                session.add(order)
        session.commit()
        logger.info("Orders inserted.")

        # Insert OrderItems (all rows)
        for _, row in df.iterrows():
            order_item = OrderItem(
                order_id=str(row['Order ID']),
                product_id=str(row['Product ID']),
                quantity_sold=int(row['Quantity Sold']),
                discount=float(row['Discount']),
                shipping_cost=float(row['Shipping Cost'])
            )
            session.add(order_item)
        session.commit()
        logger.info("OrderItems inserted.")

    except Exception as e:
        session.rollback()
        logger.error(f"Data refresh failed: {e}")
        raise

def main():
    csv_path = r'C:\Users\hephz\OneDrive\Documents\sales_data.csv'

    session = SessionLocal()
    try:
        refresh_data(session, csv_path)
        logger.info("Data load completed successfully.")
    finally:
        session.close()

if __name__ == "__main__":
    main()
