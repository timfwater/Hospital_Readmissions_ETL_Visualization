
CREATE EXTERNAL TABLE IF NOT EXISTS hospital_readmissions.gen_info (
  `provider id` INT,
  `hospital name` STRING,
  `address` STRING,
  `city` STRING,
  `state` STRING,
  `zip code` INT,
  `county name` STRING,
  `phone number` BIGINT,
  `hospital type` STRING,
  `hospital ownership` STRING,
  `emergency services` BOOLEAN,
  `meets criteria for meaningful use of ehrs` BOOLEAN,
  `hospital overall rating` STRING,
  `hospital overall rating footnote` STRING,
  `mortality national comparison` STRING,
  `mortality national comparison footnote` STRING,
  `safety of care national comparison` STRING,
  `safety of care national comparison footnote` STRING,
  `readmission national comparison` STRING,
  `readmission national comparison footnote` STRING,
  `patient experience national comparison` STRING,
  `patient experience national comparison footnote` STRING,
  `effectiveness of care national comparison` STRING,
  `effectiveness of care national comparison footnote` STRING,
  `timeliness of care national comparison` STRING,
  `timeliness of care national comparison footnote` STRING,
  `efficient use of medical imaging national comparison` STRING,
  `efficient use of medical imaging national comparison footnote` STRING,
  `location` STRING
)
STORED AS PARQUET
LOCATION 's3://glue-hospital-data/athena/gen_info/';


CREATE EXTERNAL TABLE hospital_readmissions.readmissions ( `Hospital Name` STRING,
  `Provider ID` INT, `State` STRING, `Measure Name` STRING, 
  `Number of Discharges` STRING, `Footnote` STRING, 
  `Excess Readmission Ratio` STRING, `Predicted Readmission Rate` STRING, 
  `Expected Readmission Rate` STRING, `Number of Readmissions` STRING, 
  `Start Date` STRING, `End Date` STRING
)
STORED AS PARQUET
LOCATION 's3://glue-hospital-data/athena/readmissions/'

