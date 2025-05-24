from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import pandas as pd
import datetime

DATABASE_URL = "mssql+pyodbc://username:password@localhost/sales_db?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Product(Base):
    __tablename__ = 'products'
    product_id = Column(String, primary_key=True)
    name = Column(String)
    category = Column(String)
    unit_price = Column(Float)

class Customer(Base):
    __tablename__ = 'customers'
    customer_id = Column(String, primary_key=True)
    name = Column(String)
    email = Column(String)
    address = Column(String)

class Order(Base):
    __tablename__ = 'orders'
    order_id = Column(String, primary_key=True)
    customer_id = Column(String, ForeignKey('customers.customer_id'))
    order_date = Column(Date)
    payment_method = Column(String)
    region = Column(String)
    customer = relationship("Customer")

class OrderItem(Base):
    __tablename__ = 'order_items'
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String, ForeignKey('orders.order_id'))
    product_id = Column(String, ForeignKey('products.product_id'))
    quantity_sold = Column(Integer)
    discount = Column(Float)
    shipping_cost = Column(Float)
    order = relationship("Order")
    product = relationship("Product")

def main():
    # Create tables (if not exists)
    Base.metadata.create_all(engine)
    print("Tables created or verified.")

    # Create a DB session
    session = SessionLocal()

    # Load CSV data
    df = pd.read_csv(r'C:\Users\hephz\OneDrive\Documents\sales_data.csv')

    # Insert unique Products
    products = df[['Product ID', 'Product Name', 'Category', 'Unit Price']].drop_duplicates()
    for _, row in products.iterrows():
        existing = session.query(Product).get(row['Product ID'])
        if not existing:
            product = Product(
                product_id=row['Product ID'],
                name=row['Product Name'],
                category=row['Category'],
                unit_price=row['Unit Price']
            )
            session.add(product)
    session.commit()
    print("Products inserted.")

    # Insert unique Customers
    customers = df[['Customer ID', 'Customer Name', 'Customer Email', 'Customer Address']].drop_duplicates()
    for _, row in customers.iterrows():
        existing = session.query(Customer).get(row['Customer ID'])
        if not existing:
            customer = Customer(
                customer_id=row['Customer ID'],
                name=row['Customer Name'],
                email=row['Customer Email'],
                address=row['Customer Address']
            )
            session.add(customer)
    session.commit()
    print("Customers inserted.")

    # Insert unique Orders
    orders = df[['Order ID', 'Customer ID', 'Date of Sale', 'Payment Method', 'Region']].drop_duplicates()
    for _, row in orders.iterrows():
        existing = session.query(Order).get(row['Order ID'])
        if not existing:
            order = Order(
                order_id=row['Order ID'],
                customer_id=row['Customer ID'],
                order_date=pd.to_datetime(row['Date of Sale']).date(),
                payment_method=row['Payment Method'],
                region=row['Region']
            )
            session.add(order)
    session.commit()
    print("Orders inserted.")

    # Insert OrderItems (all rows)
    for _, row in df.iterrows():
        order_item = OrderItem(
            order_id=row['Order ID'],
            product_id=row['Product ID'],
            quantity_sold=row['Quantity Sold'],
            discount=row['Discount'],
            shipping_cost=row['Shipping Cost']
        )
        session.add(order_item)
    session.commit()
    print("OrderItems inserted.")

    session.close()
    print("Data loaded successfully!")

if __name__ == "__main__":
    main()
