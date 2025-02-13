from sqlalchemy import create_engine, exc
import pandas as pd
from pathlib import Path

# Simplified loading order based on available data
LOAD_ORDER = ['products', 'categories', 'orders']

def load_to_postgres(db_url):
    """Load transformed KZ data into PostgreSQL with proper schema relationships."""
    try:
        engine = create_engine(db_url)
        data_dir = Path(__file__).parent.parent / 'data'
        input_path = data_dir / 'transformed_kz.csv'
        
        if not input_path.exists():
            raise FileNotFoundError(f"❌ Missing transformed data: {input_path}\nRun transformation step first.")

        print("📥 Loading transformed KZ data...")
        df = pd.read_csv(input_path)

        # Print the columns for debugging
        print("Available columns in the DataFrame:")
        print(df.columns)

        # Create table DataFrames based on actual data columns
        tables = {
            'products': df[['product_id', 'category_id', 'brand', 'price']].drop_duplicates('product_id'),
            
            # Create categories DataFrame with unique category_id and category pairs
            'categories': df[['category_id', 'category']].drop_duplicates(),
            
            'orders': df[['order_id', 'product_id', 'user_id', 'event_time', 'price']].drop_duplicates('order_id')
        }

        # Load data in proper order
        with engine.begin() as conn:
            for table in LOAD_ORDER:
                if table in tables:
                    print(f"🚚 Loading {table}...")
                    try:
                        tables[table].to_sql(
                            name=table,
                            con=conn,
                            if_exists='replace',
                            index=False,
                            method='multi',
                            chunksize=1000
                        )
                        print(f"✅ Loaded {len(tables[table])} rows to {table}")
                    except exc.SQLAlchemyError as e:
                        print(f"❌ Error loading {table}: {str(e)}")
                        raise

        print("\n🎉 Successfully loaded KZ product catalog!")

    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        raise

if __name__ == "__main__":
    
    DB_URL = "postgresql://postgres:ABC123@localhost:5432/E-COMMERCE DB"

    load_to_postgres(DB_URL)