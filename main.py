from mylib.lib import (
    extract,
    start_spark,
    end_spark,
    load_data,
    describe,
    query,
    transform,
)


def main():
    spark = start_spark("Birth")

    file_path = extract()
    print(file_path)
    df = load_data(spark)
    print(df)
    describe(df)
    query(
        spark, df, "SELECT births FROM Birth where year = 2000 AND month = 6", "Birth"
    )
    transform(df)

    end_spark(spark)


if __name__ == "__main__":
    main()
