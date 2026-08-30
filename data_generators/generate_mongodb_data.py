"""
MongoDB Data Generator - Users and Portfolios

DATA DOMAIN EXPLANATION:
========================
This simulates a stock trading platform's CUSTOMER DATABASE (like Robinhood, E*TRADE)

USERS Collection:
- Represents people who signed up for the trading platform
- Contains personal info: name, email, region (geography)
- risk_profile: How aggressive they invest (conservative to very_aggressive)
- kyc_status: "Know Your Customer" - are they verified to trade legally?
- metadata: Demographics like age, income, job, investment experience

PORTFOLIOS Collection:
- Represents TRADING ACCOUNTS (one user can have multiple accounts)
- portfolio_id is the KEY that links to all trading activity in BigQuery/Iceberg
- holdings: What stocks they currently own [{symbol: "AAPL", qty: 50}]
- base_ccy: Account currency (USD, EUR, etc.)
- cash_balance: Available money to trade

REAL-WORLD ANALOGY:
- users = Your bank customer profile
- portfolios = Your different bank accounts (savings, checking)
- In trading platforms: users can have retirement portfolio, day-trading portfolio, etc.
"""

# Install the following packages
# pip install faker
# pip install pymongo
# pip install python-dotenv

import os
import random
from datetime import datetime, timedelta
from faker import Faker
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

fake = Faker()

# Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://shrinivasuser:shrinivas1234@mongodbcluster.onp7sbf.mongodb.net/")
DATABASE_NAME = "fintech_trading"
NUM_USERS = 1000
NUM_PORTFOLIOS = 1500

# Stock symbols pool (30 real US stocks)
STOCK_SYMBOLS = [
    'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'WMT',
    'PG', 'JNJ', 'UNH', 'HD', 'BAC', 'MA', 'DIS', 'ADBE', 'CRM', 'NFLX',
    'CSCO', 'INTC', 'PFE', 'KO', 'PEP', 'TMO', 'ABBV', 'NKE', 'MRK', 'ORCL'
]

REGIONS = ['US', 'EU', 'APAC', 'LATAM', 'MEA']
RISK_PROFILES = ['conservative', 'moderate', 'aggressive', 'very_aggressive']
KYC_STATUS = ['verified', 'pending', 'rejected', 'under_review']
BASE_CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'AUD']


def connect_mongodb():
    """Establish MongoDB connection"""
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print(f"Connected to MongoDB at {MONGODB_URI}")
        return client
    except ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")
        print("Using mock mode - data will be printed instead of inserted")
        return None


def generate_users(num_users=NUM_USERS):
    """
    Generate user profiles
    
    Each user represents a CUSTOMER who signed up for the trading platform
    - user_id: Unique identifier (will be referenced by portfolios)
    - Demographics: age, income, occupation, investment experience
    - risk_profile: Investment strategy preference
    - kyc_status: Legal verification status (mostly verified - 85%)
    """
    users = []
    
    for i in range(num_users):
        user_id = f"USER_{str(i+1).zfill(6)}"
        created_at = fake.date_time_between(start_date='-2y', end_date='now')
        
        user = {
            'user_id': user_id,
            'name': fake.name(),
            'email': fake.unique.email(),
            'phone': fake.phone_number(),
            'region': random.choice(REGIONS),
            'risk_profile': random.choice(RISK_PROFILES),
            'kyc_status': random.choices(
                KYC_STATUS, 
                weights=[0.85, 0.08, 0.03, 0.04]  # 85% verified
            )[0],
            'created_at': created_at,
            'last_login': fake.date_time_between(
                start_date=created_at, 
                end_date='now'
            ),
            'metadata': {
                'age': random.randint(21, 75),
                'occupation': fake.job(),
                'annual_income_usd': random.randint(30000, 500000),
                'investment_experience_years': random.randint(0, 30)
            }
        }
        users.append(user)
    
    return users


def generate_portfolios(users, num_portfolios=NUM_PORTFOLIOS):
    """
    Generate trading accounts
    
    Each portfolio represents a TRADING ACCOUNT:
    - portfolio_id: THIS IS THE KEY - links to all trading activity in BigQuery/Iceberg
    - user_id: Which customer owns this account
    - holdings: Current stock positions (what they own right now)
    - cash_balance: Available cash for trading
    - base_ccy: Account currency
    
    NOTE: One user can have multiple portfolios (e.g., retirement + day-trading)
    """
    portfolios = []
    
    for i in range(num_portfolios):
        # IMPORTANT: portfolio_id format PORT_XXXXXX must match BigQuery trades!
        portfolio_id = f"PORT_{str(i+1).zfill(6)}"
        user = random.choice(users)
        
        # Generate random current holdings (3-15 different stocks)
        num_holdings = random.randint(3, 15)
        holdings = []
        
        selected_symbols = random.sample(STOCK_SYMBOLS, num_holdings)
        for symbol in selected_symbols:
            holdings.append({
                'symbol': symbol,
                'qty': random.randint(1, 1000)
            })
        
        portfolio = {
            'portfolio_id': portfolio_id,
            'user_id': user['user_id'],
            'portfolio_name': fake.catch_phrase(),
            'base_ccy': random.choice(BASE_CURRENCIES),
            'holdings': holdings,  # Current positions
            'cash_balance_usd': round(random.uniform(100, 500000), 2),
            'created_at': fake.date_time_between(
                start_date=user['created_at'], 
                end_date='now'
            ),
            'updated_at': datetime.utcnow(),
            'is_active': random.choices([True, False], weights=[0.95, 0.05])[0],
            'total_deposits_usd': round(random.uniform(1000, 1000000), 2),
            'total_withdrawals_usd': round(random.uniform(0, 500000), 2)
        }
        portfolios.append(portfolio)
    
    return portfolios


def insert_data(client, users, portfolios):
    """
    Insert generated data into MongoDB
    
    MONGODB SETUP:
    - Creates database if it doesn't exist (MongoDB creates on first write)
    - Drops existing collections for fresh data load
    - Creates indexes for query performance
    - Inserts users and portfolios
    
    INDEXES CREATED:
    - users: user_id (unique), email (unique), (region, risk_profile) compound
    - portfolios: portfolio_id (unique), user_id, (is_active, updated_at) compound
    
    WHY INDEXES:
    - Fast lookups by user_id, portfolio_id in federated queries
    - Efficient filtering by region, risk_profile
    - Optimized sorting by is_active, updated_at
    """
    # Database will be created automatically if it doesn't exist
    db = client[DATABASE_NAME]
    
    # Check if collections exist and drop them for fresh data
    existing_collections = db.list_collection_names()
    
    if 'users' in existing_collections:
        print(f"Dropping existing 'users' collection...")
        db.users.drop()
    
    if 'portfolios' in existing_collections:
        print(f"Dropping existing 'portfolios' collection...")
        db.portfolios.drop()
    
    print(f"\nDatabase '{DATABASE_NAME}' ready (created if not exists)")
    print("Collections 'users' and 'portfolios' will be created on insert")
    
    # Insert data (collections are created automatically on first insert)
    print(f"\nInserting {len(users)} users...")
    users_result = db.users.insert_many(users)
    print(f"Inserted {len(users_result.inserted_ids)} users")
    
    print(f"\nInserting {len(portfolios)} portfolios...")
    portfolios_result = db.portfolios.insert_many(portfolios)
    print(f"Inserted {len(portfolios_result.inserted_ids)} portfolios")
    
    # Create indexes for query performance
    print("\nCreating indexes for optimal query performance...")
    db.users.create_index('user_id', unique=True)
    db.users.create_index('email', unique=True)
    db.users.create_index([('region', 1), ('risk_profile', 1)])
    print("  - users: user_id (unique), email (unique), (region, risk_profile)")
    
    db.portfolios.create_index('portfolio_id', unique=True)
    db.portfolios.create_index('user_id')
    db.portfolios.create_index([('is_active', 1), ('updated_at', -1)])
    print("  - portfolios: portfolio_id (unique), user_id, (is_active, updated_at)")
    
    print(f"\nInserted {len(users_result.inserted_ids)} users")
    print(f"Inserted {len(portfolios_result.inserted_ids)} portfolios")
    
    print("\nDatabase Statistics:")
    print(f"  Total Users: {db.users.count_documents({})}")
    print(f"  Total Portfolios: {db.portfolios.count_documents({})}")
    print(f"  Active Portfolios: {db.portfolios.count_documents({'is_active': True})}")
    
    print("\nBreakdown by Region:")
    for region in REGIONS:
        count = db.users.count_documents({'region': region})
        print(f"  {region}: {count} users")
    
    print("\nRisk Profile Distribution:")
    for risk in RISK_PROFILES:
        count = db.users.count_documents({'risk_profile': risk})
        print(f"  {risk}: {count} users")


def main():
    """
    Main execution
    
    EXECUTION FLOW:
    1. Connect to MongoDB Atlas (or local MongoDB)
    2. Generate mock users and portfolios in memory
    3. Create database and collections if they don't exist
    4. Insert data and create indexes
    
    IMPORTANT:
    - MongoDB creates database automatically on first write
    - Collections are created automatically on first insert
    - This script is IDEMPOTENT - safe to run multiple times
    - Each run drops old data and creates fresh data
    """
    print("=" * 80)
    print("MongoDB Data Generator - Fintech Trading Platform")
    print("=" * 80)
    print("\nThis script will:")
    print("1. Connect to MongoDB")
    print("2. Create database 'fintech_trading' if it doesn't exist")
    print("3. Drop existing collections (if any) for fresh data")
    print("4. Insert 1,000 users and 1,500 portfolios")
    print("5. Create indexes for query performance")
    print("=" * 80)
    
    client = connect_mongodb()
    
    print(f"\nGenerating {NUM_USERS} users...")
    users = generate_users(NUM_USERS)
    
    print(f"Generating {NUM_PORTFOLIOS} portfolios...")
    portfolios = generate_portfolios(users, NUM_PORTFOLIOS)
    
    print(f"\nInserting data into MongoDB...")
    insert_data(client, users, portfolios)
    
    if client:
        client.close()
        print("\nMongoDB connection closed")
    
    print("\n" + "=" * 80)
    print("Data generation complete!")
    print("\nNext steps:")
    print("1. Verify in MongoDB: db.users.countDocuments({})")
    print("2. Run BigQuery generator: python data_generators/generate_bigquery_data.py")
    print("3. Run GCS generator: python data_generators/generate_gcs_reference_data.py")
    print("=" * 80)


if __name__ == "__main__":
    main()
