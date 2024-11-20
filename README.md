# RedFin Data Analytics Pipeline

## Overview & Architecture  
![image](https://github.com/user-attachments/assets/dc17b8f5-a201-478c-ba00-dc5b22b8370b)  

The RedFin Data Pipeline automates the extraction, transformation, and visualization of real estate data to deliver actionable insights. Utilizing tools like Apache Airflow, Amazon EC2, AWS S3, Amazon EMR, Snowflake, and Power BI, the pipeline processes over 200,000 records monthly, offering scalable, reliable data processing and robust market analysis.

### Core Components  

1. **Amazon EC2**: Hosts Apache Airflow to manage ETL workflows.  
2. **AWS S3**: Stores raw and transformed data for further processing.  
3. **Amazon EMR**: Cleans and transforms data before returning it to S3.  
4. **Snowflake**: Uses Snowpipe to automatically ingest transformed data from S3.  
5. **Power BI**: Provides interactive dashboards for detailed reporting.  

## Data Flow  

### Extraction  
- Real estate data is retrieved through the RedFin API.

### Transformation  
- **Apache Airflow**: Hosted on Amazon EC2, it orchestrates ETL workflows.  
- **Amazon EMR**: Executes data transformation, ensuring the data is clean and structured.

### Loading  
- Processed data is ingested into Snowflake via Snowpipe for storage and analysis.  

### Visualization  
- **Power BI**: Builds interactive dashboards that highlight market trends.  
![image](https://github.com/user-attachments/assets/bfbd7971-5d96-4e80-8e99-f2f7c093ecec)  

## Key Features  

1. **Scalable**: Handles over 200,000 records per month with ease.  
2. **Automated**: Streamlines data processing through fully automated ETL workflows.  
3. **Actionable Insights**: Offers comprehensive visualizations for in-depth real estate market analysis.  
