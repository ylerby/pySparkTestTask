from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Создание сессии
spark: SparkSession = SparkSession.builder.getOrCreate()


def get_products_with_categories(products: "DataFrame", categories: "DataFrame") -> "DataFrame":
    # Объединение датафреймов
    result = products.join(categories, "product_id", "left")
    # Получение необходимых столбцов
    result = result.select("product_name", "category_name")
    return result


products_data = [
    (1, "product_1"),
    (2, "product_2"),
    (3, "product_3"),
    (4, "product_4")
]
categories_data = [
    (1, "category_1"),
    (1, "category_2"),
    (2, "category_1"),
    (3, "category_3")
]

# Определение схем для датафреймов
products_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=False)
])
categories_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("category_name", StringType(), nullable=False)
])

# Создание датафреймов
products_df = spark.createDataFrame(products_data, products_schema)
categories_df = spark.createDataFrame(categories_data, categories_schema)

# Получение результата
result_df = get_products_with_categories(products_df, categories_df)

# Вывод результата
result_df.show()
