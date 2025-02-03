import vaex


df = vaex.open("C:/Users/JuanGBe/Downloads/data/*.parquet")

brands = df.groupby("brand", agg="count").sort("count")

print(brands)
print(df.schema())
