import pandas as pd
from pathlib import Path

def clean_kz_data(df):
    """Clean and transform KZ product data."""
    print("\n🔄 Cleaning and transforming KZ product data...")
    
    # Validate essential columns
    essential_cols = ['product_id', 'price']
    missing_essential = [col for col in essential_cols if col not in df.columns]
    if missing_essential:
        raise ValueError(f"Missing essential columns: {missing_essential}")

    # Clean price column
    price_col = 'price'
    if price_col in df.columns:
        try:
            df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
            df[price_col].fillna(df[price_col].median(), inplace=True)  # Fill missing prices with median
            print(f"✅ Converted {price_col} to numeric format")
            print(f"   - {price_col} stats: Mean=${df[price_col].mean():.2f}, Max=${df[price_col].max():.2f}")
        except Exception as e:
            print(f"⚠️ Failed to convert {price_col}: {str(e)}")

    # Enhanced category handling
    category_col = 'category_code'
    if category_col in df.columns:
        df['category'] = (
            df[category_col]
            .str.lower()
            .str.strip()
            .str.replace(r'\s+', ' ', regex=True)  # Clean whitespace
            .fillna('uncategorized')
        )
        df = df.drop(columns=[category_col])  # Remove original column
        print("✅ Standardized product categories")
        print(f"   - Top categories: {df['category'].value_counts().head(5).to_dict()}")
    else:
        print("\n⚠️ Warning: Missing category information!")

    # Final validation
    print(f"\n✅ Final dataset validation:")
    print(f"- Total products: {len(df):,}")
    print(f"- Columns: {list(df.columns)}")
    print(f"- Missing values per column:")
    print(df.isna().sum())
    
    return df

if __name__ == "__main__":
    data_dir = Path(__file__).parent.parent / 'data'
    
    try:
        # Load KZ data
        input_path = data_dir / 'kz.csv'
        df = pd.read_csv(input_path)
        print(f"\n📥 Loaded KZ data: {len(df):,} rows")
        
        # Process data
        df_transformed = clean_kz_data(df)
        
        # Save transformed data
        output_path = data_dir / 'transformed_kz.csv'
        df_transformed.to_csv(output_path, index=False)
        print(f"\n💾 Saved transformed data to: {output_path}")
        
    except FileNotFoundError:
        print("\n❌ Error: kz.csv not found!")
        print("   Please ensure the file is in the 'data' directory.")
    except Exception as e:
        print(f"\n❌ Transformation failed: {str(e)}")