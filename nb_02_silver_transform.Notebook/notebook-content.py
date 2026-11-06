# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

## attach lakehouse to notebook

%%configure -f
{ 
            "defaultLakehouse": {
                "name": 'lh_olist',
                "id": '0f7e660c-8270-45b9-8490-086eba9a7f9d',
                "workspaceId": '70d2278e-9339-4ad3-b727-8e1d0fbb35ca'
            }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# parameters
run_date = ""

# import Python packages
import uuid
from datetime import date
from pyspark.sql import functions as F



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
