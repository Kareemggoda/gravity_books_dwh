# 02_transform/dim_book.py

from pyspark.sql import functions as F

def build_dim_book(dataframes):
    """
    Build dim_book by joining:
    book + book_author + author + publisher + book_language
    """
    book          = dataframes["book"]
    book_author   = dataframes["book_author"]
    author        = dataframes["author"]
    publisher     = dataframes["publisher"]
    book_language = dataframes["book_language"]

    # Join author names (aggregate multiple authors per book)
    authors = book_author \
        .join(author, "author_id", "left") \
        .groupBy("book_id") \
        .agg(F.concat_ws(", ", F.collect_list("author_name")).alias("authors"))

    # Build dim_book
    dim_book = book \
        .join(authors, "book_id", "left") \
        .join(publisher, "publisher_id", "left") \
        .join(book_language, "language_id", "left") \
        .select(
            F.col("book_id").alias("book_key"),
            F.col("book_id"),
            F.col("title"),
            F.col("isbn13"),
            F.col("authors"),
            F.col("publisher_name").alias("publisher"),
            F.col("language_name").alias("language"),
            F.col("num_pages"),
            F.col("publication_date")
        ) \
        .fillna("Unknown")

    return dim_book