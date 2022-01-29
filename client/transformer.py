# Importing required modules
import numpy as np
import pandas as pd

# Importing csv file as dataframe
df = pd.read_csv("sample.csv")

# Converting records to JSON
df_json = df.to_json(
    orient="records",
    lines = True
    ).splitlines()

# Write the JSON data to a txt file
np.savetxt(
    "./output.txt",
    df_json,
    fmt="%s"
    )

