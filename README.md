# Data Process Architecture (2): Alias Resolver 

The revenue reports provided by our revenue partners show a variety of differences. Some of them contain a large list of the columns and present very detailed summary data, for example the revenue reports from Bing and Yahoo have the detailed data. And many reports from other partners just contain very few columns and present highly summarized data, for example the reports from Triple_Lift and Rubicon are very simple. Besides the reports format difference, every report uses their own defined column name stand for a similar or exactly same business measure, for example a simple concept of “revenue”, the column name could be “revenue”, “earnings”, “gross_revenue”, “estimated_revenue”, “ad_revenue”, “gorss_revenue_usd”, “net_revenue_usd”, “estimated_gross_revenue” from different reports [1]. It is obvious we need a simple solution to handle these issues effectively. It comes out as the requirement here.

## I. An Open Aliasing Solution

We designed an open aliasing system to handle the variety issues with the reports from our partners. We extract all physical concepts from all currently existing reports, and give each concept a standard name, then treat all possible names of the same concept in various reports as an alias. For example, we just use the word “Revenue” as a standard name to express a business concept of revenue, then we treat various column names like “earnings”, “gross_revenue”, “estimated_revenue”, “ad_revenue”, “gorss_revenue_usd”, “net_revenue_usd”, “estimated_gross_revenue” etc as the aliases of the “Revenue”. Whenever a column name shows up from a new partner for the concept of “Revenue”, we will that new column name as a new alias and configure it into the system. This way makes us able to handle any format of reports without making any code changes. The word “Open” means we are easy to accept and manage a variety of the report formats without any code change.


## II. Implementation on Snowflake

We have designed and implemented a “Open Aliasing System” on Snowflake platform. Here I divide the code implementation into several sections based on the functions and give some brief explanations.

### A. Generic Assistant Functions

Before we start the code implementation, we need some generic assistant functions to handle the DATA_PATTERN.

```
-------------------------------------------------------
-- Create three assistant functions
-------------------------------------------------------
-- Function 1: To parse the column presenting indicator array to a data_pattern number
-- DROP FUNCTION DATA_PATTERN(ARRAY);
CREATE OR REPLACE FUNCTION DATA_PATTERN(P ARRAY)
  RETURNS DOUBLE
  LANGUAGE JAVASCRIPT
AS 
$$
 … (see reference for code details)
$$;

-- Function 2: Search a column position in a column list
-- DROP FUNCTION COLUMN_MAP(ARRAY);
CREATE OR REPLACE FUNCTION COLUMN_MAP(P ARRAY)
  RETURNS VARIANT
  LANGUAGE JAVASCRIPT
AS 
$$
 … (see reference for code details)
$$;


-- Function 3: Get the revenue share value from a feed_contract record 
-- DROP FUNCTION REVENUE_SHARE(VARIANT, VARCHAR, FLOAT);
CREATE OR REPLACE FUNCTION REVENUE_SHARE(P VARIANT, D VARCHAR, V FLOAT)
  RETURNS FLOAT
  LANGUAGE JAVASCRIPT
AS 
$$
 … (see reference for code details)
$$;
```


### B. Data landscape and definitions

To store and data landscape and alias definitions, we need a table to hold them, the table is defined by following snowflake SQL. The column names are the standard names of the business concept, and the data types of each concept is defined in the second table.

```
-------------------------------------------------------
-- Create data landscape definition table
-------------------------------------------------------
--DROP TABLE "BI"."_CONTROL_LOGIC"."SELLSIDE_PERFORMANCE_DATA_LANDSCAPE";
CREATE TABLE "BI"."_CONTROL_LOGIC"."SELLSIDE_PERFORMANCE_DATA_LANDSCAPE" (
  "DATA_TYPE"	            	TEXT,
  "DATA_NAME"	            	TEXT,
  "DATA_PATTERN"	        		TEXT,
  "DATA_DATE"	            	TEXT,
  "DATA_HOUR"	            	TEXT,
  "DATA_TIME"	            	TEXT,
  "DATA_TIMEZONE"	        		TEXT,
  "BUSINESS_UNIT"	        		TEXT,
  "BUSINESS_UNIT_DETAIL"	TEXT,
  "PROPERTY_TYPE"	        	TEXT,
  "PROPERTY_DETAIL"	        	TEXT,
  "PLACEMENT"	            	TEXT,
  "PROVIDER"	            		TEXT,
  "NETWORK"	                		TEXT,
  "ACCOUNT"	                		TEXT,
  "PARTNER_TAG"	            	TEXT,
  "TYPE_TAG"	            		TEXT,
  "CHANNEL"	                		TEXT,
  "PRODUCT"	                		TEXT,
  "MARKET"	                		TEXT,
  "COUNTRY"	                		TEXT,
  "DEVICE"	                		TEXT,
  "BIDDER"	                		TEXT,
  "CONTRACT"	            	TEXT,
  "CURRENCY"	            	TEXT,
  "CONVERSION_RATE"	        	TEXT,
  "UP_STREAM_SHARE"	        	TEXT,
  "DOWN_STREAM_SHARE"	TEXT,
  "ECPI"	                		TEXT,
  "CPC"	                    		TEXT,
  "CPA"	                    			TEXT,
  "CTR"	                    		TEXT,
  "PTQS"	                		TEXT,
  "PAGEVIEWS"	            	TEXT,
  "REQUESTS"	            	TEXT,
  "MATCHED_REQUESTS"	    	TEXT,
  "IN_COMING_BIDS"	        	TEXT,
  "WINNING_BIDS"	        		TEXT,
  "IMPRESSIONS"	            	TEXT,
  "CLICKS"                  		TEXT,
  "ACTIONS"	                		TEXT,
  "SPAM_CLICKS"	            	TEXT,
  "AD_REVENUE"	                	TEXT,
  "REVENUE"	        			TEXT
);

-------------------------------------------------------
-- Create the standard data types for landscape data
-------------------------------------------------------
--DROP TABLE "BI"."_CONTROL_LOGIC"."SELLSIDE_PERFORMANCE_DATA_TEMPLATE";
CREATE TABLE "BI"."_CONTROL_LOGIC"."SELLSIDE_PERFORMANCE_DATA_TEMPLATE" (
	"DATA_PATTERN"			NUMBER,
	"DATA_DATE"				DATE,
	"DATA_HOUR"				NUMBER,
	"DATA_TIME"				TIMESTAMP_NTZ,
	"DATA_TIMEZONE"			TEXT,
	"BUSINESS_UNIT"			TEXT,
	"BUSINESS_UNIT_DETAIL"	TEXT,
	"PROPERTY_TYPE"			TEXT,
	"PROPERTY_DETAIL"		TEXT,
	"PLACEMENT"				TEXT,
	"PROVIDER"				TEXT,
	"NETWORK"				TEXT,
	"ACCOUNT"				TEXT,
	"PARTNER_TAG"			TEXT,
	"TYPE_TAG"				TEXT,
	"CHANNEL"				TEXT,
	"PRODUCT"				TEXT,
	"MARKET"				TEXT,
	"COUNTRY"				TEXT,
	"DEVICE"				TEXT,
	"BIDDER"				TEXT,
	"CONTRACT"				TEXT,
	"CURRENCY"				TEXT,
	"CONVERSION_RATE"		FLOAT,
	"UP_STREAM_SHARE"		FLOAT,
	"DOWN_STREAM_SHARE"		FLOAT,
	"ECPI"					FLOAT,
	"CPC"					FLOAT,
	"CPA"					FLOAT,
	"CTR"					FLOAT,
	"PTQS"					FLOAT,
	"PAGEVIEWS"				NUMBER,
	"REQUESTS"				NUMBER,
	"MATCHED_REQUESTS"		NUMBER,
	"IN_COMING_BIDS"		NUMBER,
	"WINNING_BIDS"			NUMBER,
	"IMPRESSIONS"			NUMBER,
	"CLICKS"				NUMBER,
	"ACTIONS"				NUMBER,
	"SPAM_CLICKS"			NUMBER,
	"AD_REVENUE"			FLOAT,
	"REVENUE"				FLOAT
);
```

### C. Aggregation Target Registration

For data automation, we need a table to hold the definitions of how the summary data are generated. Each row of the data stored in the table stands for one set of summary data we need to populate. We can manually insert a row of definition data into the table, for easier to use, I also developed a stored procedure which will help to generate the definition data and insert into the table.

```
-------------------------------------------------------
-- Create the target data definition table
-------------------------------------------------------
--DROP TABLE "BI"."_CONTROL_LOGIC"."DATA_AGGREGATION_TARGETS";
CREATE TABLE "BI"."_CONTROL_LOGIC"."DATA_AGGREGATION_TARGETS" 
(
TARGET_TABLE				TEXT,
BATCH_CONTROL_COLUMN		TEXT,
BATCH_CONTROL_SIZE			NUMBER,
BATCH_CONTROL_NEXT			TEXT,
BATCH_PROCESSED			TIMESTAMP_NTZ,
BATCH_PROCESSING			TIMESTAMP_NTZ,
BATCH_MICROCHUNK_CURRENT		TIMESTAMP_NTZ,
BATCH_MICROCHUNK_TYPE		TEXT,
BATCH_LAST_SCHEDULE			TIMESTAMP_NTZ,
PATTERN_COLUMNS			 ARRAY,
GROUPBY_COLUMNS		 	ARRAY,
GROUPBY_PATTERN		 	NUMBER,
GROUPBY_FLEXIBLE			BOOLEAN,
AGGREGATE_COLUMNS			ARRAY,
AGGREGATE_FUNCTIONS			ARRAY,
DEFAULT_PROCEDURE			TEXT
);


------------------------------------------
-- Aggregate target registration stored procedures
------------------------------------------
--DROP PROCEDURE BI._CONTROL_LOGIC.DATA_AGGREGATION_TARGET_SETUP  (STRING, STRING, STRING, STRING, STRING, STRING);
CREATE OR REPLACE PROCEDURE DATA_AGGREGATION_TARGET_SETUP (
  DATA_SIDE 				STRING,
  TARGET_TABLE 			STRING, 
  BATCH_CONTROL_COLUMN 	STRING,
  BATCH_SIZE_MINUTES 		STRING,
  BATCH_FUNCTION_NEXT 		STRING,
  BATCH_SCHEDULE_TYPE 	STRING
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS 
$$
 … (see reference for code details)
$$;
```
### D. Aggregation Source Registration

For data automation purposes, we need a table to hold the definitions of how the source data are transformed. Each row of the data stored in the table stands for one of the source tables for the linked target table which we need to populate. We can manually insert a row of definition data into the table, for easier to use, I also developed two stored procedures which will help to generate the definition data and insert all needed source into the table automatically.

```
-------------------------------------------------------
-- Create the target data definition table
-------------------------------------------------------
--DROP TABLE BI._CONTROL_LOGIC.DATA_AGGREGATION_SOURCES;
CREATE TABLE BI._CONTROL_LOGIC.DATA_AGGREGATION_SOURCES (
TARGET_TABLE	        		TEXT,
SOURCE_TABLE	        		TEXT,
SOURCE_ENABLED	        		BOOLEAN,
PATTERN_DEFAULT	        		NUMBER,
PATTERN_FLEXIBLE	    		BOOLEAN,
DATA_AVAILABLETIME	    	TIMESTAMP_NTZ,
DATA_CHECKSCHEDULE	    	TIMESTAMP_NTZ,
TRANSFORMATION	        		TEXT
);


------------------------------------------------------
-- Aggregate target setup stored procedures for individual SOURCE_DATA
------------------------------------------------------
--DROP PROCEDURE DATA_AGGREGATION_SOURCE_SETUP(STRING, STRING, BOOLEAN);
CREATE OR REPLACE PROCEDURE DATA_AGGREGATION_SOURCE_SETUP(
 DATA_SIDE 				STRING,
TARGET_TABLE 			STRING, 
SOURCE_TABLE 			STRING, 
SCRIPT_ONLY 			BOOLEAN
)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
AS 
$$
 … (see reference for code details)
$$;


------------------------------------------------------
-- Aggregate target setup stored procedures to loop all available source tables
------------------------------------------------------
--DROP PROCEDURE DATA_AGGREGATION_SOURCE_SETUP( STRING, BOOLEAN );
CREATE OR REPLACE PROCEDURE DATA_AGGREGATION_SOURCE_SETUP(
 DATA_SIDE 				STRING,
TARGET_TABLE 			STRING, 
SCRIPT_ONLY 			BOOLEAN
)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
AS 
$$
 … (see reference for code details)
$$;
```

### E. Aggregation Data Population

To generate the summary data, we need two stored procedures. One stored procedure is used to generate the summary SQL query for one day’s data, then execute it to populate the result into the summary table. Another one is used to loop all needed dates then call the first stored procedure to populate a period of day’s summary data.  The second stored procedure can be used to set up snowflake tasks

```
------------------------------------------------------
-- Stored procedure to summarize single day’s data from all sources
------------------------------------------------------
-- DROP PROCEDURE DATA_AGGREGATOR(STRING, STRING, BOOLEAN);
CREATE OR REPLACE PROCEDURE  DATA_AGGREGATOR (
TARGET_TABLE STRING, 
BATCH_TIMETAG STRING, 
SCRIPT_ONLY BOOLEAN
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT STRICT
AS
$$
 … (see reference for code details)
$$;


------------------------------------------------------
-- Stored procedure to loop all available source tables
------------------------------------------------------
-- Aggregate stored procedures to loop all available source tables
--DROP PROCEDURE DATA_AGGREGATOR(STRING, BOOLEAN);
CREATE OR REPLACE PROCEDURE DATA_AGGREGATOR (
TARGET_TABLE STRING, 
SCRIPT_ONLY BOOLEAN
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT STRICT
AS
$$
 … (see reference for code details)
$$;
```

## IV. An Demo Example

Here are the steps to do a full test with the solution. The detail test script to see the Reference in code file “5 Snowflake Data Aggregator - Make a full test against all objects”

## V. A Production Usage

Here are the steps for a production summary setup used for Exec Report. The detailed setup script to see the Reference in code file “6 Snowflake Data Aggregator - A production aggregation setup”.

## VI. Maintain the Mapping Data

Here are the steps to maintain a few mapping tables. The detail test script to see the Reference in code file “7 Snowflake Data Aggregator - Maintain feed contract and partner_tag usage”


## VII. Author



## VIII. License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## IX. Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc

