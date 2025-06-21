import pandas as pd
from pathlib import Path

class FixedColumnProcessor:
    """Processor that ensures correct column data mapping."""
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.master_df = None
        self.load_master_dataframe()
        
        # Normalized column names
        self.normalized_columns = [
            'Administrative', 'Bank Charges', 'Licenses & Subscriptions', 'Office Supplies',
            'Professional Fees', 'Telephone', 'Date', 'Date Header', 'Forecast Type',
            'Month', 'Quarter', 'Year', 'Insurance', 'Land Lease', 'Property Tax',
            'Utilities - Energy', 'Utilities - Water', 'Waste Disposal', 'Annualized Revenue',
            'Payroll Expenses', 'WCB', 'Advertising & Promotion', 'Client Reimbursement',
            'Sponsorship/Fundraiser', 'Car Wash Supplies', 'Equipment Rental', 'Landscaping',
            'Repairs & Maintenance', 'Shop Supplies', 'Uniforms', 'Annualized EBITDA',
            'Annualized Net Income', 'Barlow NE Revenue', 'Combined EBITDA', 'Consulting Services',
            'Delivery, Shipping', 'Eastpoint SE Revenue', 'Okotoks Revenue', 'Total Combined Net Income',
            'Total Interest Expense', 'Total Revenue', 'Other Expenses', 'Total Expenses',
            'Software Fees', 'Chemicals', 'Merchant Fees', 'Product', 'Total Direct Expenses',
            'Location', 'Rent/Lease', 'Utilities', 'Gross Profit', 'Gross Profit %',
            'Net Operating Income', 'Meals & Entertainment', 'Members (End of Month)',
            'Hydrovac Cleaning', 'PPE & Safety', 'Security', 'Tools', 'Total Indirect Expenses',
            'Travel', 'Wages', 'Windshield Tags', 'Discounts', 'Revenue Share', 'Ancillary Sales',
            'Gift Card Sales', 'Service Revenue', 'Member Wash Count', 'Direct Costs',
            '% of Revenue', 'Salary & Wages', 'Avg Revenue per Member', 'Member Wash Frequency',
            'Member Wash Volume %', 'Revenue per Member Wash', 'Revenue per Retail Wash',
            'Revenue per Total Wash', 'Member Wash Revenue', 'Retail Wash Count',
            'Retail Wash Revenue', 'Total Wash Count', 'Office Expenses', 'Amortization',
            'Interest and bank charges', 'Tounel Express', 'Vehicle', 'Interest Income',
            'Lease Revenue', 'Lube Shop Revenue', 'Office Sales', 'Self Serve Sales', 'Cash Sales'
        ]
    
    def load_master_dataframe(self):
        """Load master dataframe."""
        master_path = self.base_path / "Other" / "master_dataframe.csv"
        
        try:
            if master_path.exists():
                self.master_df = pd.read_csv(master_path)
                print(f"Master dataframe loaded: {len(self.master_df)} mappings")
            else:
                print(f"Master dataframe not found")
        except Exception as e:
            print(f"Error loading master dataframe: {e}")
    
    def extract_client_name(self, filename: str):
        """Extract client name from filename."""
        name = filename
        
        if name.endswith('.xlsx'):
            name = name[:-5]
        elif name.endswith('.xls'):
            name = name[:-4]
        
        suffixes = [' Raw Data', ' Data', ' Dashboard_01292025', ' Dashboard_01232025', ' Forecast Analysis_01022025']
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break
        
        return name.strip()
    
    def find_raw_data_sheet(self, file_path):
        """Find the correct sheet with raw data."""
        try:
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            sheet_names = excel_file.sheet_names
            
            if 'Raw Data' in sheet_names:
                return 'Raw Data'
            
            raw_data_variants = ['raw data', 'Raw data', 'RAW DATA', 'rawdata', 'RawData']
            for variant in raw_data_variants:
                if variant in sheet_names:
                    return variant
            
            return sheet_names[0]
            
        except Exception as e:
            print(f"  Error getting sheet names: {e}")
            return 0
    
    def create_client_specific_mapping(self, client_name):
        """Create mapping specific to the client with correct column names."""
        if self.master_df is None:
            return {}
        
        mapping = {}
        
        # First, use exact mappings from master dataframe
        client_mappings = self.master_df[
            (self.master_df['Client Name'].str.contains(client_name.split()[0], case=False, na=False))
        ]
        
        if len(client_mappings) > 0:
            print(f"  Using {len(client_mappings)} client-specific mappings for {client_name}")
            
            # Create mapping from client-specific rows
            for _, row in client_mappings.iterrows():
                original_col = row['Original Column Names']
                normalized_col = row['Normalized Column Name']
                if pd.notna(original_col) and pd.notna(normalized_col):
                    mapping[original_col] = normalized_col
        else:
            print(f"  No client-specific mappings found for {client_name}, using generic mappings")
            
            # Fall back to generic mappings
            for norm_name in self.normalized_columns:
                matching_rows = self.master_df[self.master_df['Normalized Column Name'] == norm_name]
                
                for _, row in matching_rows.iterrows():
                    original_col = row['Original Column Names']
                    if pd.notna(original_col):
                        if original_col not in mapping:
                            mapping[original_col] = norm_name
        
        # Add manual fixes for known column naming issues
        client_fixes = {
            'Great White Car Wash': {
                'Total - Revenue': 'Total Revenue',  # Fix the column name mapping
            },
            'Nolan Hill Car Wash': {
                'Total - Revenue': 'Total Revenue',  # Fix the column name mapping
            },
            'Dreams Car Wash': {
                'Total Location Revenue': 'Total Revenue',  # Map to standard name
            },
            'Mint Med Hat Car Wash': {
                'Total - Revenue': 'Total Revenue',  # Already correct but ensure consistency
            }
        }
        
        if client_name in client_fixes:
            print(f"  Applying manual column fixes for {client_name}")
            for original, normalized in client_fixes[client_name].items():
                mapping[original] = normalized
                print(f"    Fixed: {original} -> {normalized}")
        
        return mapping
    
    def load_excel_file(self, file_path):
        """Load Excel file with detailed mapping verification."""
        try:
            print(f"\nLoading: {file_path.name}")
            
            # Find correct sheet
            sheet_name = self.find_raw_data_sheet(file_path)
            
            # Load data
            df = pd.read_excel(file_path, engine='openpyxl', sheet_name=sheet_name)
            print(f"  Raw shape: {df.shape}")
            
            # Extract client name
            client_name = self.extract_client_name(file_path.name)
            
            # Verify key totals before processing
            if client_name == 'Mint Med Hat Car Wash' and 'Total - Revenue' in df.columns:
                original_total = df['Total - Revenue'].sum()
                print(f"  ✓ Mint Med Hat 'Total - Revenue' original total: {original_total:,.2f}")
            
            # Add metadata
            df["Client Name"] = client_name
            df["File Name"] = file_path.name
            
            print(f"  Client Name: '{client_name}'")
            print(f"  Final shape: {df.shape}")
            
            return df
            
        except Exception as e:
            print(f"  ERROR loading {file_path.name}: {e}")
            return None
    
    def create_normalized_output_with_verification(self, combined_df):
        """Create normalized output with step-by-step verification."""
        print(f"\nCreating normalized output from {len(combined_df)} records...")
        
        # Standard columns in exact order
        standard_columns = ["Record ID", "File Name", "Client Name", "Location", "Month", "Year", "Quarter", "Date", "Forecast Type"]
        
        result_df = pd.DataFrame()
        
        # Add standard columns first
        for col in standard_columns:
            if col in combined_df.columns:
                result_df[col] = combined_df[col].copy()
                print(f"  Added standard column: {col} (records with data: {combined_df[col].notna().sum()})")
        
        # Show which clients have Location data in the combined dataframe
        if 'Location' in combined_df.columns:
            print(f"\n  Location data by client:")
            for client in combined_df['Client Name'].unique():
                client_data = combined_df[combined_df['Client Name'] == client]
                location_count = client_data['Location'].notna().sum()
                unique_locations = client_data['Location'].dropna().nunique()
                sample_locations = client_data['Location'].dropna().unique()[:3]
                print(f"    {client}: {location_count} records with location, {unique_locations} unique locations")
                if len(sample_locations) > 0:
                    print(f"      Sample locations: {list(sample_locations)}")
                else:
                    print(f"      No location data found")
        
        # Process each client separately to ensure correct mappings
        clients = combined_df['Client Name'].unique()
        
        # Initialize normalized columns, but preserve existing standard columns
        sorted_normalized = sorted(self.normalized_columns)
        for norm_col in sorted_normalized:
            # Only initialize if the column doesn't already exist (to preserve standard columns like Location)
            if norm_col not in result_df.columns:
                result_df[norm_col] = None
        
        for client in clients:
            print(f"\n  Processing client: {client}")
            client_data = combined_df[combined_df['Client Name'] == client]
            client_indices = client_data.index
            
            # Get client-specific mapping
            mapping = self.create_client_specific_mapping(client)
            
            # Apply mappings for this client
            mapped_count = 0
            for original_col, normalized_col in mapping.items():
                if original_col in client_data.columns and normalized_col in sorted_normalized:
                    # Skip if this would overwrite an existing standard column with different data
                    if normalized_col in result_df.columns and normalized_col in standard_columns:
                        # Only update if the standard column is empty/null for this client
                        existing_data = result_df.loc[client_indices, normalized_col]
                        if existing_data.notna().any():
                            print(f"    Skipping {original_col} -> {normalized_col} (standard column already populated)")
                            continue
                    
                    # Ensure the normalized column exists
                    if normalized_col not in result_df.columns:
                        result_df[normalized_col] = None
                    
                    # Handle mixed data types when copying data (aggressive cleaning)
                    source_data = client_data[original_col]
                    
                    # Always clean data for financial columns to remove headers and text
                    financial_columns = ['Total Revenue', 'Total Expenses', 'Administrative', 'Annualized Revenue']
                    
                    if normalized_col in financial_columns or 'Revenue' in normalized_col or 'Expense' in normalized_col:
                        # Financial data - always convert to numeric
                        try:
                            numeric_data = pd.to_numeric(source_data, errors='coerce')
                            result_df.loc[client_indices, normalized_col] = numeric_data.values
                            
                            # Report cleaning
                            non_numeric_count = source_data.notna().sum() - numeric_data.notna().sum()
                            if non_numeric_count > 0:
                                print(f"    Cleaned {non_numeric_count} non-numeric values from {normalized_col}")
                        except Exception as e:
                            result_df.loc[client_indices, normalized_col] = source_data.values
                            print(f"    Warning: Could not clean {normalized_col}: {e}")
                    
                    elif pd.api.types.is_numeric_dtype(source_data):
                        # Pure numeric column - copy directly
                        result_df.loc[client_indices, normalized_col] = source_data.values
                    else:
                        # Mixed data types - convert to numeric for most columns
                        try:
                            numeric_data = pd.to_numeric(source_data, errors='coerce')
                            result_df.loc[client_indices, normalized_col] = numeric_data.values
                            
                            # Report cleaning for non-obvious cases
                            non_numeric_count = source_data.notna().sum() - numeric_data.notna().sum()
                            if non_numeric_count > 0:
                                print(f"    Cleaned {non_numeric_count} non-numeric values from {normalized_col}")
                        except Exception as e:
                            # Fallback - copy as-is if conversion fails
                            result_df.loc[client_indices, normalized_col] = source_data.values
                            print(f"    Warning: Could not clean {normalized_col}: {e}")
                    
                    mapped_count += 1
                    
                    # Special verification for Mint Med Hat Total Revenue
                    if client == 'Mint Med Hat Car Wash' and normalized_col == 'Total Revenue':
                        # Handle mixed data types safely
                        try:
                            original_numeric = pd.to_numeric(client_data[original_col], errors='coerce')
                            original_total = original_numeric.sum()
                            
                            mapped_numeric = pd.to_numeric(result_df.loc[client_indices, normalized_col], errors='coerce')
                            mapped_total = mapped_numeric.sum()
                            
                            print(f"    ✓ {original_col} -> {normalized_col}")
                            print(f"      Original total: {original_total:,.2f}")
                            print(f"      Mapped total: {mapped_total:,.2f}")
                            
                            if abs(original_total - mapped_total) > 0.01:
                                print(f"      ⚠ MISMATCH DETECTED!")
                            else:
                                print(f"      ✓ Totals match perfectly")
                        except Exception as e:
                            print(f"      Error verifying totals: {e}")
            
            print(f"    Applied {mapped_count} mappings for {client}")
        
        print(f"\nFinal output: {len(result_df)} records, {len(result_df.columns)} columns")
        
        # Final verification for Mint Med Hat
        mint_data = result_df[result_df['Client Name'] == 'Mint Med Hat Car Wash']
        if len(mint_data) > 0 and 'Total Revenue' in result_df.columns:
            try:
                # Handle mixed data types safely
                numeric_revenue = pd.to_numeric(mint_data['Total Revenue'], errors='coerce')
                final_total = numeric_revenue.sum()
                print(f"\nFinal Mint Med Hat Total Revenue: {final_total:,.2f}")
            except Exception as e:
                print(f"\nError calculating final Mint Med Hat Total Revenue: {e}")
        
        return result_df
    
    def run_processing(self):
        """Main processing with verification."""
        print("="*60)
        print("FIXED COLUMN PROCESSOR - DATA INTEGRITY ASSURED")
        print("="*60)
        
        # Load all Excel files
        excel_files = list(self.base_path.glob("*.xlsx")) + list(self.base_path.glob("*.xls"))
        print(f"Found {len(excel_files)} Excel files")
        
        all_dataframes = []
        
        for file_path in excel_files:
            df = self.load_excel_file(file_path)
            if df is not None:
                all_dataframes.append(df)
        
        if not all_dataframes:
            return
        
        # Combine files
        print(f"\nCombining {len(all_dataframes)} files...")
        combined_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
        combined_df["Record ID"] = range(1, len(combined_df) + 1)
        
        print(f"Combined total: {len(combined_df)} records")
        
        # Create normalized output with verification
        result_df = self.create_normalized_output_with_verification(combined_df)
        
        # Save output with explicit numeric formatting
        output_dir = self.base_path / "Other"
        output_dir.mkdir(exist_ok=True)
        
        output_path = output_dir / "raw_records_normalized_fixed.csv"
        
        # Convert numeric columns to proper numeric types before saving
        numeric_columns = [
            '% of Revenue', 'Administrative', 'Advertising & Promotion', 'Amortization', 
            'Ancillary Sales', 'Annualized EBITDA', 'Annualized Net Income', 'Annualized Revenue',
            'Avg Revenue per Member', 'Bank Charges', 'Barlow NE Revenue', 'Car Wash Supplies',
            'Cash Sales', 'Chemicals', 'Client Reimbursement', 'Combined EBITDA', 
            'Consulting Services', 'Delivery, Shipping', 'Direct Costs', 'Discounts',
            'Eastpoint SE Revenue', 'Equipment Rental', 'Gift Card Sales', 'Gross Profit',
            'Gross Profit %', 'Hydrovac Cleaning', 'Insurance', 'Interest Income',
            'Interest and bank charges', 'Land Lease', 'Landscaping', 'Lease Revenue',
            'Licenses & Subscriptions', 'Lube Shop Revenue', 'Meals & Entertainment',
            'Member Wash Count', 'Member Wash Frequency', 'Member Wash Revenue',
            'Member Wash Volume %', 'Members (End of Month)', 'Merchant Fees',
            'Net Operating Income', 'Office Expenses', 'Office Sales', 'Office Supplies',
            'Okotoks Revenue', 'Other Expenses', 'PPE & Safety', 'Payroll Expenses',
            'Product', 'Professional Fees', 'Property Tax', 'Rent/Lease',
            'Repairs & Maintenance', 'Retail Wash Count', 'Retail Wash Revenue',
            'Revenue Share', 'Revenue per Member Wash', 'Revenue per Retail Wash',
            'Revenue per Total Wash', 'Salary & Wages', 'Security', 'Self Serve Sales',
            'Service Revenue', 'Shop Supplies', 'Sponsorship/Fundraiser', 'Telephone',
            'Tools', 'Total Combined Net Income', 'Total Direct Expenses', 'Total Expenses',
            'Total Interest Expense', 'Total Revenue', 'Total Wash Count', 'Tounel Express',
            'Travel', 'Uniforms', 'Utilities', 'Utilities - Energy', 'Utilities - Water',
            'Vehicle', 'WCB', 'Wages', 'Waste Disposal', 'Windshield Tags'
        ]
        
        print(f"\nForcing numeric formatting for {len(numeric_columns)} columns...")
        
        for col in numeric_columns:
            if col in result_df.columns:
                try:
                    # Convert to numeric, errors='coerce' handles any remaining text
                    result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
                except Exception as e:
                    print(f"  Warning: Could not convert {col} to numeric: {e}")
        
        # Save with proper formatting
        result_df.to_csv(output_path, index=False, float_format='%.2f')
        
        print(f"\n✓ Saved: {output_path}")
        print(f"✓ Records: {len(result_df)}")
        print(f"✓ Columns: {len(result_df.columns)}")
        
        # Final client breakdown with key totals
        print(f"\nFinal verification by client:")
        for client in result_df['Client Name'].unique():
            client_data = result_df[result_df['Client Name'] == client]
            count = len(client_data)
            
            if 'Total Revenue' in result_df.columns:
                # Handle mixed data types in Total Revenue column
                try:
                    # Convert to numeric, errors='coerce' will turn non-numeric values to NaN
                    numeric_revenue = pd.to_numeric(client_data['Total Revenue'], errors='coerce')
                    total_revenue = numeric_revenue.sum()
                    non_numeric_count = numeric_revenue.isna().sum()
                    
                    if non_numeric_count > 0:
                        print(f"  {client}: {count} records, Total Revenue: {total_revenue:,.2f} (Note: {non_numeric_count} non-numeric values ignored)")
                    else:
                        print(f"  {client}: {count} records, Total Revenue: {total_revenue:,.2f}")
                except Exception as e:
                    print(f"  {client}: {count} records, Total Revenue: Error calculating ({e})")
            else:
                print(f"  {client}: {count} records")

def main():
    base_path = r'C:\Users\jidea\Downloads\Car Wash'
    processor = FixedColumnProcessor(base_path)
    processor.run_processing()

if __name__ == '__main__':
    main()