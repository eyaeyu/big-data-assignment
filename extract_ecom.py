import pandas as pd
from pathlib import Path 

def process_kz_data():
    """Process and analyze KZ dataset."""
    data_dir = Path(__file__).parent.parent / 'data'
    
    try:
        # Load KZ dataset
        kz_df = pd.read_csv(data_dir / 'kz.csv')
        
        print("📥 Successfully loaded dataset:")
        print(f" - KZ Data: {len(kz_df):,} rows")
        print("\n🔍 Initial Data Overview:")
        print(kz_df.info())

        # Data Cleaning
        print("\n🧹 Performing data cleaning...")
        
        # Remove duplicates based on order_id and product_id
        initial_count = len(kz_df)
        kz_df = kz_df.drop_duplicates(subset=['order_id', 'product_id'], keep='first')
        print(f"Removed {initial_count - len(kz_df)} duplicate entries")

        # Handle missing values
        print("\n🕵️ Missing Values Before Cleaning:")
        print(kz_df.isna().sum())

        # Clean price column
        kz_df['price'] = pd.to_numeric(kz_df['price'], errors='coerce')
        kz_df['price'] = kz_df['price'].fillna(kz_df['price'].median())

        # Clean categorical data
        kz_df['category_code'] = (
            kz_df['category_code']
            .str.strip()
            .str.lower()
            .fillna('uncategorized')
        )

        # Handle missing values in other columns
        kz_df['category_id'] = kz_df['category_id'].fillna(-1)  # or some other placeholder
        kz_df['brand'] = kz_df['brand'].fillna('unknown')  # or some other placeholder
        kz_df['user_id'] = kz_df['user_id'].fillna(-1)  # or some other placeholder

        # Save cleaned data
        cleaned_path = data_dir / 'cleaned_kz.csv'
        kz_df.to_csv(cleaned_path, index=False)
        print(f"\n✅ Cleaned data saved to: {cleaned_path}")

        # Basic Analysis
        print("\n📊 Basic Data Analysis:")
        print(f"Average Price: ${kz_df['price'].mean():.2f}")
        print(f"Price Range: ${kz_df['price'].min():.2f} - ${kz_df['price'].max():.2f}")
        print(f"Median Price: ${kz_df['price'].median():.2f}")

    except FileNotFoundError as e:
        print(f"❌ Error: {e}. Please ensure kz.csv is in the 'data' directory.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    process_kz_data()
    