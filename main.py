import os
import logging
import requests
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from common._helper import *

# Setup logging
logging.basicConfig(filename=r'C:\Users\roabihanna\Documents\Code Local\Projects\web scrapper\property_alert.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def send_email(sender_email, sender_password, receiver_email, subject, body, is_html=False):
    logging.info("Start the send email activity.")
    
    try:
        # Create a multipart message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject

        # Attach the body of the email (plain or HTML)
        if is_html:
            msg.attach(MIMEText(body, 'html'))
        else:
            msg.attach(MIMEText(body, 'plain'))

        # SMTP server setup for Hotmail/Outlook
        smtp_server = 'smtp-mail.outlook.com'
        smtp_port = 587

        # Sending email via SMTP
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()

        logging.info("Email sent successfully!")
    
    except Exception as e:
        logging.error(f"Failed to send email. Error: {e}")

def fetch_data(parameters, num_pages=50):
    logging.info("Start the fetch data activity.")
    try:
        results = []
        for page_nbr in range(1, num_pages + 1):
            url = (f"https://www.realestate.com.lb/laravel/api/member/properties?sort=created_at&ct=1"
                   f"&tp=1&lc={parameters['city']}&direction=desc{parameters['max_price']}&pg={page_nbr}")
            response = requests.get(url)
            response.raise_for_status()  # Check for HTTP request errors
            data = response.json()
            if data['data']['docs']:
                results.extend(data['data']['docs'])
                logging.info(f"iteration {page_nbr} is not empty.")
            else:
                logging.warning(f"iteration {page_nbr} is empty, fetching process is interrupted")
                break
        logging.info("fetch data successfull.")
        return results
    except Exception as e:
        logging.error(f"Failed to fetch data: {e}")
        return []

def update_csv_file(df, csv_file_path):
    logging.info("Starting the update CSV activity.")
    try:
        if os.path.exists(csv_file_path):
            chunk_size = 1000  # Read in chunks of 1000 rows
            problematic_rows = []

            # Read in chunks and append to df_existing
            df_existing = pd.DataFrame()
            for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, on_bad_lines='warn'):
                df_existing = pd.concat([df_existing, chunk], ignore_index=True)

            # Convert 'ID' columns to string
            df['ID'] = df['ID'].astype(str)
            df_existing['ID'] = df_existing['ID'].astype(str)

            # Find new rows
            new_ids = df[~df['ID'].isin(df_existing['ID'])]

            if not new_ids.empty:
                new_ids.to_csv(csv_file_path, mode='a', header=False, index=False)
                logging.info(f"Added {len(new_ids)} new rows to the CSV file.")
                return f"Added {len(new_ids)} new rows to the CSV file."
            else:
                logging.info("No new rows to add.")
                return "No new rows to add."
        else:
            # If file doesn't exist, write the full DataFrame
            df.to_csv(csv_file_path, index=False)
            logging.info(f"File created and {len(df)} rows added to the CSV file.")
            return f"File created and {len(df)} rows added to the CSV file."
    except Exception as e:
        logging.error(f"Failed to update CSV file: {e}")
        return f"Failed to update CSV file: {e}"

def clean_data(df):
    logging.info("Start the clean data activity.")
    try:
        # Process DataFrame
        df['agent.last_name'] = df['agent.last_name'].fillna('')
        df['agent.full_name'] = (df['agent.first_name'] + ' ' + df['agent.last_name']).str.strip()
        df['price_per_m2'] = round(df['price'] / df['area'])
        df.rename(columns={'id': 'ID'}, inplace=True)

        data_cities={'969': 'Achrafiyeh',
                    '213':'Gemayzeh',
                    '569':'Mar Mikhael',
                    '28': 'Beirut',
                    '577': 'Badaro'}
        
    # Replace the community_id with the city names
        df['community'] = df['community_id'].astype(str).replace(data_cities)

        # If district_id should also be mapped to data_cities
        df['district'] = df['district_id'].astype(str).replace(data_cities)

        # Define needed columns
        needed_columns = [
            'ID', 'price', 'area', 'price_per_m2','community', 'district', 'bedroom_value', 'bathroom_value', 
            'furnished', 'title_en', 'client.display_name', 'client.phone', 
            'agent.full_name', 'agent.phone', 'created_at', 'reference'
        ]
        df = df[needed_columns]
        logging.info("Data cleaning successfull.")
        return df
    except Exception as e:
        logging.error(f"Failed to clean the data: {e}")

def send_alert(df,message, max_price, max_bedrooms, max_price_per_m2, sender_email, sender_password, receiver_email):
    logging.info("Start the send alert acrivity.")
    try:
        # Filter the DataFrame based on price, number of bedrooms, and price per square meter
        filtered_df = df[
            (df['price'] <= max_price) & 
            (df['bedroom_value'] <= max_bedrooms) & 
            (df['price_per_m2'] <= max_price_per_m2)
        ]
        
        # Group by the community (city) column and calculate the average price per square meter for each city
        average_price_per_city = df.groupby('community')['price_per_m2'].mean().reset_index()
        # Rename the columns for clarity
        average_price_per_city.columns = ['City', 'Average Price per m²']
        # Start with the header for average price per square meter
        body = H2("Average Price per Square Meter for Each City:")
        
        # Add city-wise average price per square meter
        for index, row in average_price_per_city.iterrows():
            body += bold(f"{row['City']}: ") + f"{round(row['Average Price per m²'], 2)} $/m²" + line_break()

        # Include CSV update message
        body += line_break() + bold(f"CSV Update: {message}") + line_break()

        # Add properties matching the criteria
        if not filtered_df.empty:
            total_rows = filtered_df.shape[0]
            body += H2(f"Properties Matching Your Criteria ({total_rows}):") 

            for _, row in filtered_df.iterrows():
                # Format the created_at field to be more readable
                created_at = row['created_at']
                created_at_obj = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
                formatted_created_at = created_at_obj.strftime("%B %d, %Y %I:%M %p")

                # Add property details with formatted text and line breaks
                body += bold("Created at: ") + f"{formatted_created_at}" + line_break()
                body += bold("Title: ") + f"{row['title_en']}" + line_break()
                body += bold("Price: ") + f"{row['price']}$" + line_break()
                body += bold("Community: ") + f"{row['community']}" + line_break()
                body += bold("Bedrooms: ") + f"{row['bedroom_value']}" + line_break()
                body += bold("Area: ") + f"{row['area']} m²" + line_break()
                body += bold("Price per m²: ") + f"{round(row['price_per_m2'], 2)} $/m²" + line_break()
                body += bold("Contact: ") + f"{row['client.display_name']} ({row['client.phone']})" + line_break()
                body += bold("Reference: ") + f"{row['reference']}" + line_break()

                # Add a divider between properties
                body += "=" * 50 + line_break()

            subject = "Property Alert: Matches Found"
        else:
            subject = "Property Alert: No Matches Found"
            body += "No matching properties found."

        # Send the email with the filtered properties
        send_email(sender_email, sender_password, receiver_email, subject, body, True)

    except Exception as e:
        logging.error(f"Error in sending alert: {e}")


# Main process
def main():
    try:
        parameters = {
            'city': 'c969-c213-c569-c577',
            'max_price': '&mapr=500000'
        }
        
        # Fetch data from the API
        data = fetch_data(parameters)

        # Convert data to DataFrame
        df = pd.json_normalize(data)

        # Clean the data
        df = clean_data(df)

        # Update the CSV file with new data
        csv_file_path = "realestate_results.csv"
        message = update_csv_file(df, csv_file_path)

        # Send alert with filtered properties
        max_price = 150000
        max_bedrooms = 1
        max_price_per_m2 = 2600
        send_alert(df, message,  max_price, max_bedrooms, max_price_per_m2, 'emailFrom@outlook.com', 'pass', 'emailTo@outlook.com')
        logging.info('='*50)

    except Exception as e:
        logging.error(f"Error in main process: {e}")

if __name__ == "__main__":
    main()
