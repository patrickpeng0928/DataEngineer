# Python Pandas

## I/O
```python
import pandas as pd
df = pd.read_csv(input_file, sep = " ", header = None)
df.columns = ['hostname', '-', '--', 'datetime', 'timezone', 'request', 'response_code', 'bytes']
grouped = df.groupby('hostname')
result = grouped['hostname'].count()
output_file = "records_" + input_file
result.to_csv(output_file, sep = ' ')
```
