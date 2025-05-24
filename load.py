import pandas as pd

chunk_size = 100_000
chunks = pd.read_csv('C:\\Users\\hephz\\OneDrive\\Documents\\sales_data.csv', chunksize=chunk_size)

for chunk in chunks:
    print(chunk.head())
    break
