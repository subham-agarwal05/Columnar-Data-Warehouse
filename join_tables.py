import pandas as pd
import os

def main():
    # Define paths
    base_dir = r"c:\Users\subha\Desktop\Data Modelling Project\Columnar-Data-Warehouse\Data"
    fact_file = os.path.join(base_dir, "fact_sales_normalized.csv")
    
    dim_campaigns_file = os.path.join(base_dir, "dim_campaigns.csv")
    dim_customers_file = os.path.join(base_dir, "dim_customers.csv")
    dim_dates_file = os.path.join(base_dir, "dim_dates.csv")
    dim_products_file = os.path.join(base_dir, "dim_products.csv")
    dim_salespersons_file = os.path.join(base_dir, "dim_salespersons.csv")
    dim_stores_file = os.path.join(base_dir, "dim_stores.csv")
    
    output_file = os.path.join(base_dir, "fact_sales_denormalized_generated.csv")
    
    print("Loading datasets...")
    # Load Fact Table
    df_fact = pd.read_csv(fact_file)
    
    # Load Dimension Tables
    df_campaigns = pd.read_csv(dim_campaigns_file)
    df_customers = pd.read_csv(dim_customers_file)
    df_dates = pd.read_csv(dim_dates_file)
    df_products = pd.read_csv(dim_products_file)
    df_salespersons = pd.read_csv(dim_salespersons_file)
    df_stores = pd.read_csv(dim_stores_file)
    
    print("Performing joins...")
    # Join dimensional data into fact table based on surrogate keys
    df_joined = df_fact.merge(df_customers, on='customer_sk', how='left')
    df_joined = df_joined.merge(df_products, on='product_sk', how='left')
    df_joined = df_joined.merge(df_stores, on='store_sk', how='left')
    df_joined = df_joined.merge(df_salespersons, on='salesperson_sk', how='left')
    df_joined = df_joined.merge(df_campaigns, on='campaign_sk', how='left')
    
    #Store Manager Join
    print("Joining store manager...")
    df_joined = df_joined.merge(df_salespersons, left_on='store_manager_sk', right_on='salesperson_sk', how='left', suffixes=('', '_store_manager'))
    df_joined.rename(columns={
        'salesperson_id_store_manager': 'store_manager_id',
        'salesperson_name_store_manager': 'store_manager_name',
        'salesperson_role_store_manager': 'store_manager_role',
        'salesperson_sk_store_manager': 'store_manager_sk'
    }, inplace=True)

    #Campaign Start Date Join
    print("Joining campaign dates...")
    df_joined = df_joined.merge(df_dates, left_on='start_date_sk', right_on='date_sk', how='left')
    df_joined.rename(columns={
        'full_date': 'campaign_start_date',
        'date_sk': 'campaign_start_date_sk',
        'year': 'campaign_start_year',
        'month': 'campaign_start_month',
        'day': 'campaign_start_day',
        'weekday': 'campaign_start_weekday',
        'quarter': 'campaign_start_quarter'
    }, inplace=True)

    # Campaign End Date Join
    print("Joining campaign dates...")
    df_joined = df_joined.merge(df_dates, left_on='end_date_sk', right_on='date_sk', how='left')
    df_joined.rename(columns={
        'full_date': 'campaign_end_date',
        'date_sk': 'campaign_end_date_sk',
        'year': 'campaign_end_year',
        'month': 'campaign_end_month',
        'day': 'campaign_end_day',
        'weekday': 'campaign_end_weekday',
        'quarter': 'campaign_end_quarter'
    }, inplace=True)
    
    # Drop all surrogate keys (columns ending with '_sk')
    sk_cols = [col for col in df_joined.columns if col.endswith('_sk')]
    print(f"Dropping surrogate keys: {sk_cols}")
    df_joined.drop(columns=sk_cols, inplace=True)
    
    print(f"Exporting to {output_file}...")
    df_joined.to_csv(output_file, index=False)
    
    print("Done! Joined table has shape:", df_joined.shape)

if __name__ == "__main__":
    main()
