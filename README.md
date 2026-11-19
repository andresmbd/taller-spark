# Taller de ventas con PySpark

Pipeline local para perfilar y limpiar los archivos de clientes, productos, pedidos y líneas de pedido; calcular ventas netas y guardar resultados en Parquet.

## Requisitos

- Python 3.12.
- Java 17, 21 o 25, instalado y disponible en `PATH` (requerido por Spark).
- PySpark 4.3.0, versión fijada para este proyecto.

En este workspace, el entorno virtual está en `.venv/`.

## Ejecutar

Desde la carpeta raíz del proyecto:

```bash
source .venv/bin/activate
python src/sales_pipeline.py
```

El programa lee las rutas relativas a la raíz del proyecto, muestra en consola el perfil, los tipos convertidos, las anomalías y los controles de calidad. Las escrituras usan `overwrite`, por lo que cada ejecución reconstruye las salidas sin acumular archivos de ejecuciones anteriores.

## Datos de entrada

Los CSV principales se encuentran en `data/`:

- `clientes.csv`: clientes y país.
- `productos.csv`: categoría, precio y estado del producto.
- `pedidos.csv`: fecha, estado y canal del pedido.
- `detalle_pedido.csv`: producto, cantidad y descuento por línea.

Los lotes incrementales JSONL están en `data/incremental/`:

- `pedidos_incrementales.jsonl`
- `detalle_incremental.jsonl`

Las relaciones son `pedidos.customer_id → clientes.customer_id`, `detalle_pedido.order_id → pedidos.order_id` y `detalle_pedido.product_id → productos.product_id`.

## Limpieza y criterios

- Los CSV se leen inicialmente como texto y se convierten explícitamente: fechas y marcas de tiempo, precios y descuentos decimales, cantidades enteras y `active` booleano.
- Se perfilan nulos, claves duplicadas, tipos convertidos y referencias huérfanas.
- Los duplicados por clave se colapsan. En los datos de ejemplo, los duplicados de clientes y pedidos son filas exactas.
- El estado `deliverd` se corrige a `delivered`.
- Cantidades no positivas, descuentos fuera del intervalo `[0, 1]`, precios nulos o no positivos, fechas mal formadas y referencias huérfanas no participan en ventas; las filas afectadas se guardan en las carpetas de cuarentena correspondientes.
- Los pedidos `cancelled` se excluyen de ventas; se incluyen los demás estados válidos.
- El correo faltante se conserva como nulo porque no es necesario para las uniones ni el cálculo de ventas.

El importe neto de cada línea se calcula como:

```text
quantity * unit_price * (1 - discount_pct)
```

Se usan decimales con cuatro posiciones para importes. El importe se calcula a nivel de línea antes de agregar, y las tablas de dimensiones se reducen a una fila por clave para evitar multiplicar valores en las uniones.

## Salidas

El pipeline escribe Parquet bajo `output/`:

| Ruta | Contenido |
| --- | --- |
| `output/ventas_mensuales/` | Ventas netas por mes, país, canal y categoría. |
| `output/detalle_ventas/` | Líneas válidas enriquecidas, con `net_amount`. |
| `output/pedidos_estado/` | Estado de pedidos tras combinar histórico e incremental. |
| `output/detalle_incremental/` | Líneas leídas del lote incremental, deduplicadas por `line_id`. |
| `output/cuarentena_lineas/` | Líneas con cantidades o descuentos inválidos, referencias huérfanas o producto con precio inválido. |
| `output/cuarentena_pedidos/` | Pedidos con cliente ausente o huérfano. |
| `output/cuarentena_timestamps/` | Pedidos con marca de tiempo no interpretable. |
| `output/cuarentena_productos/` | Productos con precio nulo o no positivo. |

Spark puede crear varios archivos `part-*.parquet` dentro de cada ruta. Se leen apuntando a la carpeta, por ejemplo:

```python
ventas = spark.read.parquet("output/ventas_mensuales")
ventas.show()
```

## Lote incremental

El estado incremental combina los pedidos históricos con `pedidos_incrementales.jsonl`, particiona por `order_id` y conserva la fila con el `updated_at` más reciente. Después escribe el estado con `overwrite` en `output/pedidos_estado/`. Así, volver a procesar el mismo lote no agrega copias y una actualización tardía, como la de `O00005`, prevalece sobre su versión histórica.

Las líneas incrementales se leen y deduplican por `line_id`, y se guardan por separado. No se incorporan al agregado mensual histórico de esta versión del taller.

## Controles de calidad

El programa comprueba y muestra:

1. Que la suma de `net_amount` en el detalle válido coincide exactamente con la suma agregada en `ventas_mensuales`.
2. Que las líneas después de las uniones tienen `line_id` únicos y dimensiones requeridas no nulas.
3. En el incremental, que el número de pedidos coincide con el número de `order_id` distintos y que la actualización tardía de `O00005` es la versión seleccionada.

Con los datos actuales, la ejecución reconcilia `71.680.894,3550` en el detalle y el agregado, con `29.434` líneas válidas. El resultado puede variar si cambian los CSV.
