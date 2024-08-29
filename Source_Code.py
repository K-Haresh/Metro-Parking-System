import pandas as pd
from datetime import datetime, timedelta, time

def count_entries_exits_night_halt_per_date(file_path, sheet_name):
    # Read the Excel file
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # Filter out records where 'Vehicle Status' is 'Pass Sale' or 'Entry'
    df = df[~df['Vehicle Status'].isin(['Pass Sale', 'Entry'])]

    # Ensure 'Entry Date' and 'Revenue Date' are in datetime format with the correct format
    df['Entry Date'] = pd.to_datetime(df['Entry Date'], format='%d/%m/%Y %I:%M:%S %p', dayfirst=True)
    df['Revenue Date'] = pd.to_datetime(df['Revenue Date'], format='%d/%m/%Y', dayfirst=True, errors='coerce')

    # Adjust dates based on the categorization rule (1:00 AM boundary)
    df['Entry Date'] = df['Entry Date'].apply(lambda x: x - pd.DateOffset(days=1) if x.hour < 1 else x)
    df['Exit Date'] = df['Revenue Date'] + pd.DateOffset(days=1)

    # Extract date part from 'Entry Date' and 'Revenue Date' and create new columns 'Date' and 'Date2'
    df['Date'] = df['Entry Date'].dt.date
    df['Date2'] = df['Revenue Date'].dt.date

    # Determine night halts considering multiple night halts
    def calculate_night_halts(row):
        entry_date = row['Entry Date']
        exit_date = row['Exit Date']

        if pd.isna(exit_date):
            exit_date = datetime.now()  # Assuming the vehicle is still parked if exit date is not available

        night_halt_dates = []
        previous_night_halt_dates = []
        current_date = entry_date.date()

        while current_date <= exit_date.date():
            temp_date = datetime.combine(current_date, time(1, 1))
            if (entry_date <= temp_date < exit_date) or (entry_date < temp_date + timedelta(days=1) <= exit_date):
                if current_date == entry_date.date() and time(1, 1) <= entry_date.time() <= time(4, 29):
                    night_halt_dates.append(current_date)
                else:
                    previous_night_halt_dates.append(current_date)
            current_date += timedelta(days=1)

        return night_halt_dates, previous_night_halt_dates

    # Apply the night halt calculation to each row
    df[['Night Halt Dates', 'Previous Night Halt Dates']] = df.apply(
        lambda row: pd.Series(calculate_night_halts(row)), axis=1)

    # Explode the night halt dates to separate rows
    exploded_df = df.explode('Night Halt Dates')
    exploded_df_prev = df.explode('Previous Night Halt Dates')

    # Group by 'Station' and 'Night Halt Dates' to count unique vehicles present in night halts
    grouped_night_halts = exploded_df.groupby(['Station', 'Night Halt Dates']).size().unstack(fill_value=0).stack()
    grouped_prev_night_halts = exploded_df_prev.groupby(['Station', 'Previous Night Halt Dates']).size().unstack(fill_value=0).stack()

    # Determine same day exits and night halt exits
    df['Same Day Exit'] = df.apply(
        lambda row: 1 if row['Date'] == row['Date2'] else 0,
        axis=1
    )
    df['Night Halt Exit'] = df.apply(
        lambda row: 1 if row['Date'] != row['Date2'] and not pd.isnull(row['Date2']) else 0,
        axis=1
    )

    # Group by 'Station' and 'Date' to count entries, exits, night halts, and revenue per station per date
    grouped_entries = df.groupby(['Station', 'Date']).size().unstack(fill_value=0).stack()
    grouped_same_day_exits = df.groupby(['Station', 'Date'])['Same Day Exit'].sum()
    grouped_night_halt_exits = df.groupby(['Station', 'Date2'])['Night Halt Exit'].sum()

    # Aggregate 'Amount' and 'Other Payment Amount' to get revenue per station and date using 'Revenue Date'
    grouped_revenue = df.groupby(['Station', 'Date2']).agg({
        'Amount': 'sum',
        'Other Payment Amount': 'sum'
    })

    # Additional groups for passenger types
    grouped_cmr = df[df['Passenger Type'] == 'CMRL Passengers'].groupby(['Station', 'Date']).size().unstack(fill_value=0).stack()
    grouped_non_cmr = df[df['Passenger Type'] == 'Non CMRL Passengers'].groupby(['Station', 'Date']).size().unstack(fill_value=0).stack()

    # Updated calculation for Previous Day Night Halt Count
    def calculate_previous_day_night_halt(row):
        entry_date = row['Entry Date']
        exit_date = row['Exit Date']
        revenue_date = row['Revenue Date']
        
        previous_day_night_halt_dates = []
        
        # Consider dates between entry and exit date for previous night halts
        current_date = entry_date + timedelta(days=1)
        
        while current_date < exit_date:
            if current_date <= revenue_date and current_date != revenue_date:
                previous_day_night_halt_dates.append(current_date.date())
            current_date += timedelta(days=1)
        
        return previous_day_night_halt_dates
    
    # Apply the previous day night halt calculation to each row
    df['Previous Day Night Halt Dates'] = df.apply(calculate_previous_day_night_halt, axis=1)
    
    # Explode the previous day night halt dates to separate rows
    exploded_df_prev_day_night_halts = df.explode('Previous Day Night Halt Dates')
    
    # Group by 'Station' and 'Previous Day Night Halt Dates' to count unique vehicles
    grouped_previous_day_night_halts = exploded_df_prev_day_night_halts.groupby(['Station', 'Previous Day Night Halt Dates']).size().unstack(fill_value=0).stack()

    results = []
    all_stations = df['Station'].unique()

    for station in all_stations:
        station_entries = grouped_entries.get(station, pd.Series())
        station_same_day_exits = grouped_same_day_exits.get(station, pd.Series())
        station_night_halt_exits = grouped_night_halt_exits.get(station, pd.Series())
        station_night_halts = grouped_night_halts.get(station, pd.Series())
        station_prev_night_halts = grouped_prev_night_halts.get(station, pd.Series())
        station_previous_day_night_halts = grouped_previous_day_night_halts.get(station, pd.Series())
        station_revenue = grouped_revenue.loc[station] if station in grouped_revenue.index.levels[0] else pd.DataFrame(columns=['Amount', 'Other Payment Amount'])
        station_cmr_pass = grouped_cmr.get(station, pd.Series())
        station_non_cmr_pass = grouped_non_cmr.get(station, pd.Series())

        for date in sorted(set(station_entries.index) | set(station_same_day_exits.index) | set(station_night_halt_exits.index) | set(station_night_halts.index) | set(station_prev_night_halts.index) | set(station_previous_day_night_halts.index) | set(station_revenue.index) | set(station_cmr_pass.index) | set(station_non_cmr_pass.index)):  # Union of all dates from all counts
            entry_count = station_entries.get(date, 0)
            same_day_exit_count = station_same_day_exits.get(date, 0)
            night_halt_exit_count = station_night_halt_exits.get(date, 0)
            prev_night_halt_count = station_prev_night_halts.get(date, 0)
            previous_day_night_halt_count = station_previous_day_night_halts.get(date, 0)
            night_halt_count = night_halt_exit_count + previous_day_night_halt_count  # Updated calculation for Night Halt Count
            revenue_amount = (station_revenue.loc[date, 'Amount'] if date in station_revenue.index else 0) + \
                             (station_revenue.loc[date, 'Other Payment Amount'] if date in station_revenue.index else 0)
            cmr_pass = station_cmr_pass.get(date, 0)
            non_cmr_pass = station_non_cmr_pass.get(date, 0)
            results.append([station, date, entry_count, same_day_exit_count, night_halt_exit_count, night_halt_count, previous_day_night_halt_count, revenue_amount, cmr_pass, non_cmr_pass])

    result_df = pd.DataFrame(results, columns=['Station', 'Date', 'Entry Count', 'Same Day Exit Count', 'Previous day entry today exit', 'Night Halt Count', 'Previous Day entry no exit', 'Revenue', 'CMRL Passengers Count', 'Non-CMRL Passengers Count'])
    
    # Remove records where every field is zero
    result_df = result_df[(result_df[['Entry Count', 'Same Day Exit Count', 'Previous day entry today exit', 'Night Halt Count', 'Previous Day entry no exit', 'Revenue', 'CMRL Passengers Count', 'Non-CMRL Passengers Count']] != 0).any(axis=1)]

    # Write the result to a new Excel file
    result_df.to_excel('#Enter your output file path here.xlsx', index=False)

# Provide the path to your Excel file and the sheet name
file_path = '#Enter your input file path here .xlsx'
sheet_name = '#Enter your sheet name here'

count_entries_exits_night_halt_per_date(file_path, sheet_name)
