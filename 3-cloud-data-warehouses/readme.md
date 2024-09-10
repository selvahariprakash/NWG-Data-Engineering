# Project : Data Warehouse 

#### Client : Sparkify 

#### Business domain : Music streaming 

#### Projec Scope: Move the process and data to cloud. 

        1. Build ETL pipeline to extract data from S3. 

        2. Stage them in Redshift. 

        3. Transform data into a set of dimensional tables for analytics. 

#### Project Datasets :  

        1. Song data: s3://udacity-dend/song_data 

        2. Log data: s3://udacity-dend/log_data 

        3. Log metadata: s3://udacity-dend/log_json_path.json 

#### Project Approach :  

        1. Design schema for fact and dimension tables. 

        2. Create statement for each table in sql_queries.py 

        3. Drop statement for each table in sql_queries.py 

        4. Copy statement for each stage files in sql_queries.py 

        5. Insert statement for each table in sql_queries.py 

        6. Python scripts to execute the DDL queries from sql_queries.py in create_tables.py  

        7. Python scripts to execute the DML queries from sql_queries.py in etl.py 

#### Access S3 and check the data:
   
		1. Create new IAM user on AWS account with administrator access. 

		2. Attach existing policies directly. 

		3. Copy access key and secret key to data warehouse configuration dwh.config 

[CLUSTER] 
HOST=rds-cluster-1.________.us-west-2.redshift.amazonaws.com 
DB_NAME=sparkify 
DB_USER=shp 
DB_PASSWORD=Passw0rd 
DB_PORT=5439 

[IAM_ROLE] 
ARN=arn:aws:iam::_________:role/derole1 

[S3] 
LOG_DATA='s3://udacity-dend/log_data' 
LOG_JSONPATH='s3://udacity-dend/log_json_path.json' 
SONG_DATA='s3://udacity-dend/song_data' 

[AWS] 
KEY= AKIAxxxxxxxxxGZC43RP7 
SECRET= lYgReZlxxxxxxxxxxxxxxxxxxxxxxxxxxxxaMqN/yr1z 

[DWH]  
DWH_CLUSTER_TYPE=multi-node 
DWH_NUM_NODES=4 
DWH_NODE_TYPE=dc2.large 
DWH_IAM_ROLE_NAME=derole1 
DWH_CLUSTER_IDENTIFIER=rds-cluster-1 
DWH_DB=sparkify 
DWH_DB_USER=shp 
DWH_DB_PASSWORD=Passw0rd 
DWH_PORT=5439 

		4. Load data warehouse parameters from file dwh.config to create EC2, IAM, Redshift instances. 

		5. To interact with EC2 and S3, utilize boto3.resource; for IAM and Redshift, use boto3.client 

		6. Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly) arn:aws:iam::051_______452:role/derole1 

		7. Create a RedShift Cluster 

		8. Connect to the cluster.

		9. Run the create_tables.py file to drop the table if exists and create the required tables to stages the files and analytical tables to load the file. 

		10. Run the etl.py file to copy the files from s3 bucket to stage and then insert in to analytical tables. 
