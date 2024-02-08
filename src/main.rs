use deltalake::{
    arrow::{
        array::{Float64Array, Int32Array, StringArray},
        datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema},
        record_batch::RecordBatch,
    },
    SchemaDataType, SchemaField,
};
// use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::datafusion::execution::context::SessionContext;
use deltalake::datafusion::logical_expr::col;
use deltalake::datafusion::logical_expr::lit;
use deltalake::operations::collect_sendable_stream;
use deltalake::operations::create::CreateBuilder;
use deltalake::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use deltalake::{protocol::SaveMode, DeltaOps};

use std::{collections::HashMap, sync::Arc};

fn get_table_columns() -> Vec<SchemaField> {
    vec![
        SchemaField::new(
            "id".to_string(),
            SchemaDataType::primitive("integer".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "name".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "double".to_string(),
            SchemaDataType::primitive("double".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "float".to_string(),
            SchemaDataType::primitive("float".to_string()),
            true,
            HashMap::new(),
        ),
    ]
}

fn get_table_batches() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("name", ArrowDataType::Utf8, true),
        Field::new("double", ArrowDataType::Float64, true),
        Field::new("float", ArrowDataType::Float64, true),
    ]));

    let int_values = Int32Array::from(vec![1, 2]);
    let str_values = StringArray::from(vec!["A", "B"]);
    let db_values = Float64Array::from(vec![10000000000.2, 10000000000.1]);
    // let db_values = Float64Array::from(vec![10000000000.22222, 10000000000.11111]);
    let ft_values = Float64Array::from(vec![0.2, 0.1]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(int_values),
            Arc::new(str_values),
            Arc::new(db_values),
            Arc::new(ft_values),
        ],
    )
    .unwrap()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    let table_dir = "./test_create";

    // The operations module uses a builder pattern that allows specifying several options
    // on how the command behaves. The builders implement `Into<Future>`, so once
    // options are set you can run the command using `.await`.
    let table = CreateBuilder::new()
        .with_location(table_dir)
        .with_columns(get_table_columns())
        .with_table_name("my_table")
        .with_comment("A table to show how delta-rs works")
        .await?;

    assert_eq!(table.version(), 0);

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let batch = get_table_batches();
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_writer_properties(writer_properties)
        .await?;

    assert_eq!(table.version(), 1);

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    // To overwrite instead of append (which is the default), use `.with_save_mode`:
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Overwrite)
        .with_writer_properties(writer_properties)
        .await?;

    assert_eq!(table.version(), 2);
    let (_table, stream) = DeltaOps(table).load().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("Table after 1st Version \n {:?}", data);

    //Update table based on predicate.
    let (table, _metrics) = DeltaOps(_table)
        .update()
        .with_predicate(col("id").eq(lit(1)))
        .with_update("name", lit("Z"))
        .await
        .unwrap();

    let (_table, stream) = DeltaOps(table).load().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("Table after update \n {:?}", data);
    let table = deltalake::open_table_with_version(table_dir, 2)
        .await
        .unwrap();

    let (_table, stream) = DeltaOps(table).load().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;
    // let ctx = SessionContext::new();
    // ctx.register_table("demo", Arc::new(table)).unwrap();
    // let df = ctx.sql("SELECT * FROM demo").await.unwrap();
    println!(
        "Read previous version before update - version 2 \n {:?}",
        data
    );
    // df.show().await?;
    // let (_table, stream) = DeltaOps(table).load().await?;
    // let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    let table = deltalake::open_table(table_dir).await.unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("demo", Arc::new(table)).unwrap();
    let df = ctx
        .sql("SELECT SUM(float) as problemFloat, SUM(double) as problemDouble FROM demo")
        .await
        .unwrap();

    println!("Aggregate test sql query with rounding error \n ");

    df.show().await?;

    let df = ctx
        .sql("SELECT cast(SUM(cast(float as decimal(6,2))) as float) as correctFloat, cast(SUM(cast(double as decimal(16,6))) as double) as correctDouble FROM demo")
        .await
        .unwrap();

    println!("Aggregate test sql query with casting\n ");
    df.show().await?;
    Ok(())
}
