# ELT-Pipeline-SEC-API-Python
Extract, Load and Transformation of SEC financial Dataset 

    1 Downloads Zip file from the sec financial datset link / api > https://www.sec.gov/files/dera/data/financial-statement-data-sets/2023q4.zip
    2 Extracts Zip file 
    3 Creates the necessary tables in PostgreSQL (num, sub, pre, tag)
    4 Inserts the tables into DB

[Financial Statements Data Documentation](https://www.sec.gov/files/aqfs.pdf)

*note: creates additional folder in current dir to store and extract the zip file*

#### Next Steps: **(In progress)**
-    Remove unwanted columns
-    Change Datatypes
-    Create and Implement Views
-    ....
