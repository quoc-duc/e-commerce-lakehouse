from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
from datetime import datetime
import polars as pl

COMPUTE_KIND = "SQL"
LAYER = "bronze"

@asset(
    description="Load customers from local CSV as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "customer"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_customer(context) -> Output[pl.DataFrame]:
    df_data = pl.read_csv("/opt/dagster/app/data/olist_customers_dataset.csv")
    context.log.info(f"CSV loaded with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "customers",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )



@asset(
    description="Load sellers from local CSV as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "seller"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)

def bronze_seller(context) -> Output[pl.DataFrame]:
    df_data = pl.read_csv("/opt/dagster/app/data/olist_sellers_dataset.csv")
    context.log.info(f"CSV loaded with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "sellers",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )



@asset(
    description="Load products from local CSV as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "product"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)

def bronze_product(context) -> Output[pl.DataFrame]:
    df_data = pl.read_csv("/opt/dagster/app/data/olist_products_dataset.csv")
    context.log.info(f"CSV loaded with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "products",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )



@asset(
    description="Load orders from local CSV as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "order"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)

def bronze_order(context) -> Output[pl.DataFrame]:
    df_data = pl.read_csv("/opt/dagster/app/data/olist_orders_dataset.csv")
    context.log.info(f"CSV loaded with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "orders",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )



@asset(
    description="Load order items from local CSV as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "orderitem"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_order_item(context) -> Output[pl.DataFrame]:
    df_data = pl.read_csv("/opt/dagster/app/data/olist_order_items_dataset.csv")
    context.log.info(f"CSV loaded with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "order_items",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )



@asset(
    description="Load payments from local CSV as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "payment"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)

def bronze_payment(context) -> Output[pl.DataFrame]:
    df_data = pl.read_csv("/opt/dagster/app/data/olist_order_payments_dataset.csv")
    context.log.info(f"CSV loaded with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "payments",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )



@asset(
    description="Load order reviews from local CSV as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "orderreview"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)

def bronze_order_review(context) -> Output[pl.DataFrame]:
    df_data = pl.read_csv("/opt/dagster/app/data/olist_order_reviews_dataset.csv")
    context.log.info(f"CSV loaded with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "order_reviews",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


@asset(
    description="Load product category translations from local CSV as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "productcategory"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_product_category(context) -> Output[pl.DataFrame]:
    df_data = pl.read_csv("/opt/dagster/app/data/product_category_name_translation.csv")
    context.log.info(f"CSV loaded with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "product_category",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# --------------------#

@asset(
    description="Load geolocation data from local CSV as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "geolocation"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_geolocation(context) -> Output[pl.DataFrame]:
    df_data = pl.read_csv("/opt/dagster/app/data/olist_geolocation_dataset.csv")
    context.log.info(f"CSV loaded with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "geolocation",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )