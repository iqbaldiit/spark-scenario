import pandas as pd
import pandas as ps

# sample data
data = ["India", "Pakistan", "SriLanka"]
columns=["teams"]
df = pd.DataFrame(data, columns=columns)

print()
print("==========Input Data=============")

print(df.head())
print()
print("==========Expected output=============")