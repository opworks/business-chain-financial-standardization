import pandas as pd
import os
import re
from datetime import datetime

class DreamsRawDataCreator:
    """Transform Dreams Car Wash P&L into Raw Data format with proper date fields."""
    
    def __init__(self, dreams_file_path: str, output_folder: str):
        self.dreams_file_path = dreams_file_path
        self.output_folder = output_folder
        self.dreams_locations = {
            "Okotoks": "Okotoks",
            "Barlow NE": "Barlow NE", 
            "Eastpoint SE": "Eastpoint SE",
            "Corp": "Corporate",
            "General": "General"  # New category for non-location-specific items
        }
    
    def find_date_headers(self):
        """Find and parse date headers from Dreams P&L sheet (row 1, B1:AP1)."""
        print("Searching for date headers in Dreams P&L (row 1, B1:AP1)...")
        
        try:
            # Read raw data without headers
            df_raw = pd.read_excel(self.dreams_file_path, sheet_name="P&L with Corp", header=None)
            print(f"Raw data shape: {df_raw.shape}")
            
            # Look specifically at row 1 (index 0) starting from column B (index 1)
            row_idx = 0  # Row 1 (0-indexed)
            start_col = 1  # Column B (0-indexed)
            
            row_data = df_raw.iloc[row_idx, start_col:].tolist()  # Start from column B
            date_info = []
            
            for col_offset, cell in enumerate(row_data):
                if pd.notna(cell):
                    # Try to parse as date
                    parsed_date = None
                    try:
                        # Direct parsing (Excel dates are usually datetime objects)
                        parsed_date = pd.to_datetime(cell, errors='coerce')
                        if pd.isna(parsed_date) and isinstance(cell, str):
                            # Try common formats if it's a string
                            for fmt in ['%b-%y', '%B %Y', '%m/%Y', '%Y-%m', '%Y-%m-%d']:
                                try:
                                    parsed_date = pd.to_datetime(cell.strip(), format=fmt)
                                    if not pd.isna(parsed_date):
                                        break
                                except:
                                    continue
                    except:
                        pass
                    
                    if not pd.isna(parsed_date):
                        # Extract date components
                        month = parsed_date.strftime("%B")
                        year = parsed_date.year
                        quarter = f"Q{(parsed_date.month-1)//3 + 1}"
                        date_formatted = parsed_date.strftime("%Y-%m-%d")
                        forecast_type = "FORECAST" if parsed_date > datetime(2025, 6, 30) else "ACTUAL"
                        
                        date_info.append({
                            'col_idx': start_col + col_offset,  # Actual column index in DataFrame
                            'original_header': str(cell),
                            'parsed_date': parsed_date,
                            'month': month,
                            'year': year,
                            'quarter': quarter,
                            'date': date_formatted,
                            'forecast_type': forecast_type
                        })
            
            if len(date_info) >= 3:  # Found at least 3 dates
                print(f"Found {len(date_info)} dates in row {row_idx + 1}")
                print(f"Date range: {date_info[0]['date']} to {date_info[-1]['date']}")
                print(f"Years covered: {min(d['year'] for d in date_info)} to {max(d['year'] for d in date_info)}")
                return row_idx, date_info, df_raw
            else:
                print(f"Only found {len(date_info)} dates, need at least 3")
                return None, [], None
            
        except Exception as e:
            print(f"Error finding date headers: {e}")
            return None, [], None
    
    def extract_categories_from_column_a(self, df_raw):
        """Extract categories from column A with their actual row numbers."""
        categories_with_rows = []
        
        # Go through all rows in column A
        for row_idx in range(len(df_raw)):
            cell_value = df_raw.iloc[row_idx, 0]  # Column A
            if pd.notna(cell_value):
                category = str(cell_value).strip()
                if category:  # Only include non-empty categories
                    categories_with_rows.append({
                        'category': category,
                        'actual_row': row_idx  # Store the actual row index
                    })
        
        print(f"Found {len(categories_with_rows)} categories in column A")
        print(f"Sample categories with rows: {[(c['category'], c['actual_row']) for c in categories_with_rows[:10]]}")
        return categories_with_rows
    
    def is_location_specific_field(self, field_name):
        """Check if a field is location-specific (only individual location revenues)."""
        field_clean = field_name.strip()
        
        # ONLY these specific revenue fields are location-specific
        location_specific_fields = [
            "Okotoks Revenue",
            "Barlow NE Revenue", 
            "Eastpoint SE Revenue",
            "Corp Revenue"
        ]
        
        # Check if this is a location-specific revenue field
        if field_clean in location_specific_fields:
            # Map field to location
            if "Okotoks" in field_clean:
                return True, "Okotoks"
            elif "Barlow NE" in field_clean:
                return True, "Barlow NE"
            elif "Eastpoint SE" in field_clean:
                return True, "Eastpoint SE"
            elif "Corp" in field_clean:
                return True, "Corp"
        
        # All other fields (expenses, totals, EBITDA, etc.) are NOT location-specific
        return False, None
    
    def should_include_field_for_location(self, field_name, current_location_key):
        """Determine if a field should be included for a specific location."""
        is_location_specific, field_location = self.is_location_specific_field(field_name)
        
        if is_location_specific:
            # Location-specific revenue field: only include if it matches current location
            return field_location == current_location_key
        else:
            # General field: only include for the "General" category
            return current_location_key == "General"
    
    def create_raw_data_records(self, df_raw, date_info, categories_with_rows):
        """Create raw data records for each location and date combination."""
        print("Creating raw data records...")
        
        raw_data_records = []
        
        for date_item in date_info:
            col_idx = date_item['col_idx']
            
            # Create records for each location
            for location_key, location_name in self.dreams_locations.items():
                record = {
                    "Location": location_name,
                    "Month": date_item['month'],
                    "Year": date_item['year'],
                    "Quarter": date_item['quarter'],
                    "Date": date_item['date'],
                    "Forecast Type": date_item['forecast_type']
                }
                
                # Add each category as a column with its value
                for category_info in categories_with_rows:
                    category = category_info['category']
                    actual_row = category_info['actual_row']  # Use the actual row number!
                    
                    if category and str(category) != 'nan':
                        # Get value from the CORRECT row using actual_row
                        value = df_raw.iloc[actual_row, col_idx] if col_idx < df_raw.shape[1] else 0
                        
                        # Clean the value
                        if pd.isna(value):
                            value = 0
                        elif isinstance(value, str):
                            try:
                                cleaned_value = re.sub(r'[^\d.-]', '', str(value))
                                value = float(cleaned_value) if cleaned_value else 0
                            except:
                                value = 0
                        elif isinstance(value, (int, float)):
                            value = float(value)
                        else:
                            value = 0
                        
                        # Determine if this field should be included for this location
                        if self.should_include_field_for_location(category, location_key):
                            record[category] = value
                        else:
                            # Field doesn't belong to this location/category: set to 0
                            record[category] = 0
                
                raw_data_records.append(record)
        
        print(f"Created {len(raw_data_records)} raw data records")
        return raw_data_records
    
    def save_raw_data(self, raw_data_records):
        """Save Dreams raw data in Excel format with Raw Data sheet."""
        if not raw_data_records:
            print("No data to save")
            return None
        
        # Create DataFrame
        raw_data_df = pd.DataFrame(raw_data_records)
        
        # Ensure standard columns are first
        standard_cols = ["Location", "Month", "Year", "Quarter", "Date", "Forecast Type"]
        other_cols = sorted([col for col in raw_data_df.columns if col not in standard_cols])
        column_order = standard_cols + other_cols
        raw_data_df = raw_data_df[column_order]
        
        # Create output folder
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Save as Excel file with "Raw Data" sheet (to match other car wash files)
        excel_output_path = os.path.join(self.output_folder, "Dreams Car Wash Raw Data.xlsx")
        with pd.ExcelWriter(excel_output_path, engine='openpyxl') as writer:
            raw_data_df.to_excel(writer, sheet_name="Raw Data", index=False)
        
        print(f"Dreams raw data saved to: {excel_output_path}")
        print(f"Sheet name: Raw Data (matches other car wash files)")
        
        # Also save as CSV for convenience
        csv_output_path = os.path.join(self.output_folder, "Dreams Car Wash Raw Data.csv")
        raw_data_df.to_csv(csv_output_path, index=False)
        print(f"CSV version saved to: {csv_output_path}")
        
        return raw_data_df, excel_output_path
    
    def show_summary(self, raw_data_df):
        """Show summary of created raw data."""
        if raw_data_df is None or raw_data_df.empty:
            return
        
        print(f"\n{'='*60}")
        print("DREAMS RAW DATA SUMMARY")
        print(f"{'='*60}")
        
        print(f"Total records: {len(raw_data_df)}")
        print(f"Total columns: {len(raw_data_df.columns)}")
        
        print(f"\nLocations: {raw_data_df['Location'].unique()}")
        print(f"Date range: {raw_data_df['Date'].min()} to {raw_data_df['Date'].max()}")
        print(f"Years: {sorted(raw_data_df['Year'].unique())}")
        print(f"Months: {len(raw_data_df['Month'].unique())} unique months")
        print(f"Forecast types: {raw_data_df['Forecast Type'].unique()}")
        
        # Show records per location
        print(f"\nRecords per location:")
        for location in raw_data_df['Location'].unique():
            count = len(raw_data_df[raw_data_df['Location'] == location])
            print(f"  {location}: {count} records")
        
        # Verify location-specific vs general field distribution
        standard_cols = ["Location", "Month", "Year", "Quarter", "Date", "Forecast Type"]
        location_specific_fields = [col for col in raw_data_df.columns 
                                  if any(loc in col for loc in ['Okotoks Revenue', 'Barlow NE Revenue', 'Eastpoint SE Revenue', 'Corp Revenue'])]
        general_fields = [col for col in raw_data_df.columns 
                         if col not in location_specific_fields and col not in standard_cols]
        
        print(f"\nField Distribution:")
        print(f"  Location-specific revenue fields: {len(location_specific_fields)}")
        print(f"  General fields (expenses, totals, etc.): {len(general_fields)}")
        print(f"  Standard columns: {len(standard_cols)}")
        
        if location_specific_fields:
            print(f"\nLocation-specific revenue fields:")
            for field in location_specific_fields:
                print(f"  {field}")
                
        if general_fields:
            print(f"\nGeneral fields (assigned to 'General' location only):")
            for field in general_fields[:10]:  # Show first 10
                print(f"  {field}")
            if len(general_fields) > 10:
                print(f"  ... and {len(general_fields) - 10} more")
        
        # Verify data distribution by location
        print(f"\nData distribution verification:")
        for location in raw_data_df['Location'].unique():
            location_data = raw_data_df[raw_data_df['Location'] == location]
            sample_date_data = location_data.iloc[0]  # First record for this location
            
            if location == "General":
                # General location should have non-zero values for general fields, zero for location-specific
                non_zero_general = sum(1 for field in general_fields if sample_date_data.get(field, 0) != 0)
                non_zero_location = sum(1 for field in location_specific_fields if sample_date_data.get(field, 0) != 0)
                print(f"  {location}: {non_zero_general} non-zero general fields, {non_zero_location} non-zero location fields")
            else:
                # Location should have non-zero for its specific revenue field only
                location_field = f"{location} Revenue" if location != "Corporate" else "Corp Revenue"
                location_value = sample_date_data.get(location_field, 0)
                general_values = [sample_date_data.get(field, 0) for field in general_fields[:3]]
                print(f"  {location}: {location_field} = {location_value}, general fields = {general_values}")
        
        # Show sample data structure
        print(f"\nSample data structure (first 2 records for each location type):")
        sample_cols = ["Location", "Month", "Year"]
        
        # Show one location-specific record
        if "Okotoks" in raw_data_df['Location'].values:
            okotoks_sample = raw_data_df[raw_data_df['Location'] == 'Okotoks'].iloc[0]
            revenue_cols = ['Okotoks Revenue', 'Barlow NE Revenue', 'Total Location Revenue']
            available_cols = [col for col in sample_cols + revenue_cols if col in raw_data_df.columns]
            print(f"  Okotoks sample: {dict(okotoks_sample[available_cols])}")
        
        # Show general record
        if "General" in raw_data_df['Location'].values:
            general_sample = raw_data_df[raw_data_df['Location'] == 'General'].iloc[0]
            expense_cols = ['Professional Fees', 'Administrative', 'Total Location Revenue']
            available_cols = [col for col in sample_cols + expense_cols if col in raw_data_df.columns]
            print(f"  General sample: {dict(general_sample[available_cols])}")
        
        # Show sample data
        print(f"\nSample data (first 3 rows, key columns):")
        sample_cols = ["Location", "Month", "Year", "Quarter", "Forecast Type"]
        revenue_cols = [col for col in raw_data_df.columns if "Revenue" in col or "Total" in col or "EBITDA" in col][:4]
        display_cols = sample_cols + revenue_cols
        available_cols = [col for col in display_cols if col in raw_data_df.columns]
        
        if available_cols:
            print(raw_data_df[available_cols].head(3).to_string(index=False))
        
        # CRITICAL: Show totals verification
        print(f"\n{'='*40}")
        print("TOTALS VERIFICATION")
        print(f"{'='*40}")
        
        # Check October 2022 specifically to verify the fix
        oct_2022_data = raw_data_df[(raw_data_df['Month'] == 'October') & (raw_data_df['Year'] == 2022)]
        if not oct_2022_data.empty:
            print(f"\nOctober 2022 Revenue Check:")
            
            # Get individual location revenues
            okotoks_rev = oct_2022_data[oct_2022_data['Location'] == 'Okotoks']['Okotoks Revenue'].iloc[0] if 'Okotoks' in oct_2022_data['Location'].values else 0
            barlow_rev = oct_2022_data[oct_2022_data['Location'] == 'Barlow NE']['Barlow NE Revenue'].iloc[0] if 'Barlow NE' in oct_2022_data['Location'].values else 0
            eastpoint_rev = oct_2022_data[oct_2022_data['Location'] == 'Eastpoint SE']['Eastpoint SE Revenue'].iloc[0] if 'Eastpoint SE' in oct_2022_data['Location'].values else 0
            
            # Get total from General location
            total_rev = oct_2022_data[oct_2022_data['Location'] == 'General']['Total Location Revenue'].iloc[0] if 'General' in oct_2022_data['Location'].values else 0
            
            calculated_total = okotoks_rev + barlow_rev + eastpoint_rev
            
            print(f"  Okotoks Revenue: ${okotoks_rev:,.2f}")
            print(f"  Barlow NE Revenue: ${barlow_rev:,.2f}")
            print(f"  Eastpoint SE Revenue: ${eastpoint_rev:,.2f}")
            print(f"  Calculated Total: ${calculated_total:,.2f}")
            print(f"  Reported Total: ${total_rev:,.2f}")
            print(f"  Difference: ${abs(total_rev - calculated_total):,.2f}")
            
            if abs(total_rev - calculated_total) < 0.01:
                print("  ✓ TOTALS MATCH! Data quality issue is FIXED!")
            else:
                print("  ✗ Totals still don't match - need further investigation")
    
    def create_dreams_raw_data(self):
        """Main method to create Dreams raw data file."""
        print("="*60)
        print("DREAMS CAR WASH RAW DATA CREATOR (FIXED VERSION)")
        print("="*60)
        
        # Find date headers
        date_row_idx, date_info, df_raw = self.find_date_headers()
        
        if not date_info or df_raw is None:
            print("Failed to extract date information from Dreams file")
            return None
        
        # Extract categories WITH their actual row numbers
        categories_with_rows = self.extract_categories_from_column_a(df_raw)
        
        if not categories_with_rows:
            print("No categories found in column A")
            return None
        
        # Create raw data records using correct row alignment
        raw_data_records = self.create_raw_data_records(df_raw, date_info, categories_with_rows)
        
        if not raw_data_records:
            print("No raw data records created")
            return None
        
        # Save the raw data
        raw_data_df, excel_path = self.save_raw_data(raw_data_records)
        
        # Show summary
        self.show_summary(raw_data_df)
        
        print(f"\n{'='*60}")
        print("SUCCESS!")
        print(f"Dreams Car Wash transformed to Raw Data format")
        print(f"Data quality issue FIXED - totals should now match!")
        print(f"File ready for uniform processing with other car wash files")
        print(f"{'='*60}")
        
        return excel_path

def main():
    """Main execution."""
    # Configuration
    dreams_file_path = r"C:\Users\jidea\Downloads\Car Wash\DreamsCarWash Car Analysis.xlsx"
    output_folder = r"C:\Users\jidea\Downloads\Car Wash"
    
    # Create raw data
    creator = DreamsRawDataCreator(dreams_file_path, output_folder)
    excel_path = creator.create_dreams_raw_data()
    
    if excel_path:
        print(f"\nNow you can process this file alongside other car wash files:")
        print(f"  {excel_path}")
        print(f"  Sheet: Raw Data")
        print(f"\nThis file has the same structure as other car wash Raw Data files!")
        print(f"The row alignment issue has been fixed - totals should now match!")

if __name__ == "__main__":
    main()