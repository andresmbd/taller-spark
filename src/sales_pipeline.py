"""Limpieza, unión y agregación de ventas del taller con PySpark."""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

DATA = "data"
OUT = "output"
MONEY = DecimalType(18, 4)
PCT = DecimalType(9, 6)

spark = (SparkSession.builder
         .master("local[*]")
         .appName("sales-quality-pipeline")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

spark.conf.set("spark.sql.session.timeZone", "UTC")


def csv(name):
    return spark.read.option("header", True).option("inferSchema", False).csv(f"{DATA}/{name}.csv")


customers_raw = csv("clientes")
products_raw = csv("productos")
orders_raw = csv("pedidos")
items_raw = csv("detalle_pedido")

customers = (customers_raw
    .withColumn("signup_date", F.to_date("signup_date"))
    .withColumn("country", F.upper(F.trim("country")))
    .dropDuplicates(["customer_id"]))

products = (products_raw
    .withColumn("unit_price", F.expr("try_cast(unit_price as decimal(18,4))"))
    .withColumn("active", F.expr("try_cast(active as boolean)"))
    .dropDuplicates(["product_id"]))
orders = (orders_raw
    .withColumn("order_ts", F.try_to_timestamp("order_ts", F.lit("yyyy-MM-dd'T'HH:mm:ssX")))
    .withColumn("updated_at", F.try_to_timestamp("updated_at", F.lit("yyyy-MM-dd'T'HH:mm:ssX")))
    .withColumn("status", F.lower(F.trim("status")))
    .withColumn("status", F.when(F.col("status") == "deliverd", "delivered").otherwise(F.col("status")))
    .dropDuplicates(["order_id"]))
items = (items_raw
    .withColumn("quantity", F.expr("try_cast(quantity as int)"))
    .withColumn("discount_pct", F.expr("try_cast(discount_pct as decimal(9,6))"))
    .dropDuplicates(["line_id"]))


for name, df, key in [("clientes", customers_raw, "customer_id"), ("productos", products_raw, "product_id"),
                       ("pedidos", orders_raw, "order_id"), ("detalle_pedido", items_raw, "line_id")]:
    print(f"PERFIL {name}: filas={df.count()}, duplicados_{key}={df.count()-df.select(key).distinct().count()}")
    print("nulos por columna:", {c: df.filter(F.col(c).isNull() | (F.trim(F.col(c)) == "")).count() for c in df.columns})
print("Tipos después de conversión:")
for name, df in [("clientes", customers), ("productos", products), ("pedidos", orders), ("detalle", items)]:
    print(name)
    df.printSchema()
print("Estados con typo deliverd:", orders_raw.filter(F.lower(F.trim("status")) == "deliverd").count())
print("Pedidos con timestamp mal formado:", orders.filter(F.col("order_ts").isNull()).count())

print("Pedidos con cliente huérfano:", orders.join(customers.select("customer_id"), "customer_id", "left_anti").count())
print("Líneas con pedido huérfano:", items.join(orders.select("order_id"), "order_id", "left_anti").count())
print("Líneas con producto huérfano:", items.join(products.select("product_id"), "product_id", "left_anti").count())
print("Líneas con cantidad inválida:", items.filter(F.col("quantity").isNull() | (F.col("quantity") <= 0)).count())
print("Líneas con descuento inválido:", items.filter(F.col("discount_pct").isNull() | (F.col("discount_pct") < 0) | (F.col("discount_pct") > 1)).count())
print("Productos con precio inválido:", products.filter(F.col("unit_price").isNull() | (F.col("unit_price") <= 0)).count())
print("Decisiones: duplicados exactos por clave se colapsan; 'deliverd' se corrige a 'delivered';")
print("cantidades <= 0, descuentos fuera de [0,1], precios <= 0 y referencias huérfanas se ponen en cuarentena.")
print("Las órdenes canceladas no se consideran ventas; los estados restantes se incluyen.")



valid_customers = customers.filter(F.col("customer_id").isNotNull())
valid_products = products.filter(F.col("product_id").isNotNull() & F.col("unit_price").isNotNull() & (F.col("unit_price") > 0))
valid_orders = orders.filter(F.col("order_id").isNotNull() & F.col("order_ts").isNotNull() & F.col("customer_id").isNotNull()
                             & (F.trim("customer_id") != "") & (F.col("status") != "cancelled"))
valid_items = items.filter(F.col("line_id").isNotNull() & F.col("quantity").isNotNull() & (F.col("quantity") > 0)
                           & F.col("discount_pct").isNotNull() & F.col("discount_pct").between(0, 1))


order_context = (valid_orders.join(valid_customers.select("customer_id", "country"), "customer_id", "inner")
                 .select("order_id", "order_ts", "channel", "country"))
line_sales = (valid_items.join(valid_products.select("product_id", "category", "unit_price"), "product_id", "inner")
              .join(order_context, "order_id", "inner")
              .withColumn("net_amount", (F.col("quantity").cast(MONEY) * F.col("unit_price") * (F.lit(1).cast(PCT) - F.col("discount_pct"))).cast(MONEY))
              .withColumn("month", F.date_trunc("month", "order_ts").cast("date")))



bad_item_ids = (items.filter(F.col("quantity").isNull() | (F.col("quantity") <= 0)
                             | F.col("discount_pct").isNull() | ~F.col("discount_pct").between(0, 1))
                .withColumn("reject_reason", F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "quantity_not_positive_or_invalid")
                            .otherwise("discount_out_of_range_or_invalid")))
orphan_order_items = (items.join(orders.select("order_id").distinct(), "order_id", "left_anti")
                      .withColumn("reject_reason", F.lit("orphan_order")))
orphan_product_items = (items.join(products.select("product_id").distinct(), "product_id", "left_anti")
                        .withColumn("reject_reason", F.lit("orphan_product")))
invalid_price_products = products.filter(F.col("unit_price").isNull() | (F.col("unit_price") <= 0)).select("product_id")
products.filter(F.col("unit_price").isNull() | (F.col("unit_price") <= 0)).write.mode("overwrite").parquet(f"{OUT}/cuarentena_productos")
invalid_price_items = (items.join(invalid_price_products, "product_id", "inner")
                       .withColumn("reject_reason", F.lit("invalid_product_price")))
invalid_customer_orders = (orders.join(customers.select("customer_id").distinct(), "customer_id", "left_anti")
                           .withColumn("reject_reason", F.lit("orphan_or_missing_customer")))
invalid_timestamp_orders = (orders.filter(F.col("order_ts").isNull())
                            .withColumn("reject_reason", F.lit("invalid_order_timestamp")))
quarantine = (bad_item_ids.unionByName(orphan_order_items, allowMissingColumns=True)
              .unionByName(orphan_product_items, allowMissingColumns=True)
              .unionByName(invalid_price_items, allowMissingColumns=True))
quarantine.write.mode("overwrite").parquet(f"{OUT}/cuarentena_lineas")
invalid_customer_orders.write.mode("overwrite").parquet(f"{OUT}/cuarentena_pedidos")
invalid_timestamp_orders.write.mode("overwrite").parquet(f"{OUT}/cuarentena_timestamps")

monthly = (line_sales.groupBy("month", "country", "channel", "category")
           .agg(F.sum("net_amount").cast(MONEY).alias("net_sales")))


detail_total = line_sales.agg(F.sum("net_amount").alias("total")).first()["total"]
aggregate_total = monthly.agg(F.sum("net_sales").alias("total")).first()["total"]
assert detail_total == aggregate_total, f"Reconciliación fallida: {detail_total} != {aggregate_total}"
print(f"QC1 reconciliación detalle/agregado: {detail_total} == {aggregate_total}")

line_count = line_sales.count()
distinct_lines = line_sales.select("line_id").distinct().count()
assert line_count == distinct_lines, f"Join duplicó líneas: {line_count} vs {distinct_lines}"
assert line_sales.filter(F.col("country").isNull() | F.col("channel").isNull() | F.col("category").isNull()).count() == 0
print(f"QC2 unicidad de líneas tras joins: {line_count} líneas únicas")

monthly.write.mode("overwrite").parquet(f"{OUT}/ventas_mensuales")
line_sales.write.mode("overwrite").parquet(f"{OUT}/detalle_ventas")
monthly.orderBy("month", "country", "channel", "category").show(20, truncate=False)



inc_path = f"{DATA}/incremental/pedidos_incrementales.jsonl"
inc = (spark.read.json(inc_path)
       .withColumn("order_ts", F.try_to_timestamp("order_ts", F.lit("yyyy-MM-dd'T'HH:mm:ssX")))
       .withColumn("updated_at", F.try_to_timestamp("updated_at", F.lit("yyyy-MM-dd'T'HH:mm:ssX"))))
inc_items = (spark.read.json(f"{DATA}/incremental/detalle_incremental.jsonl")
             .withColumn("quantity", F.expr("try_cast(quantity as int)"))
             .withColumn("discount_pct", F.expr("try_cast(discount_pct as decimal(9,6))")))
hist = orders.select("order_id", "customer_id", "order_ts", "status", "channel", "updated_at")
rank = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc_nulls_last())
latest = (hist.unionByName(inc).withColumn("rn", F.row_number().over(rank)).filter("rn=1").drop("rn"))
latest.write.mode("overwrite").parquet(f"{OUT}/pedidos_estado")
inc_items.dropDuplicates(["line_id"]).write.mode("overwrite").parquet(f"{OUT}/detalle_incremental")
latest_count = latest.count()
latest_keys = latest.select("order_id").distinct().count()
assert latest_count == latest_keys, f"Upsert incremental duplicó pedidos: {latest_count} vs {latest_keys}"
late_update = latest.filter(F.col("order_id") == "O00005").select("updated_at", "status").first()
assert late_update is not None and late_update["updated_at"] == inc.filter(F.col("order_id") == "O00005").first()["updated_at"]
print(f"QC incremental: {latest_count} pedidos, {latest_keys} claves únicas; O00005 actualizado a {late_update['updated_at']} ({late_update['status']}).")
print(f"Líneas incrementales leídas: {inc_items.count()}; claves de línea deduplicadas por line_id.")
print("Pedidos incrementales: estado actualizado idempotentemente en output/pedidos_estado")
print("El JSONL repetido no duplica: unión por order_id + row_number por updated_at descendente.")

spark.stop()
