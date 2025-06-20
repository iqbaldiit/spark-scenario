'''

	Table: library_books

	+------------------+---------+
	| Column Name      | Type    |
	+------------------+---------+
	| book_id          | int     |
	| title            | varchar |
	| author           | varchar |
	| genre            | varchar |
	| publication_year | int     |
	| total_copies     | int     |
	+------------------+---------+
	book_id is the unique identifier for this table.
	Each row contains information about a book in the library, including the total number of copies owned by the library.
	Table: borrowing_records

	+---------------+---------+
	| Column Name   | Type    |
	+---------------+---------+
	| record_id     | int     |
	| book_id       | int     |
	| borrower_name | varchar |
	| borrow_date   | date    |
	| return_date   | date    |
	+---------------+---------+
	record_id is the unique identifier for this table.
	Each row represents a borrowing transaction and return_date is NULL if the book is currently borrowed and hasn't been returned yet.

	Write a solution to find all books that are currently borrowed (not returned) and have zero copies available in the library.
	A book is considered currently borrowed if there exists a borrowing record with a NULL return_date
	Return the result table ordered by current borrowers in descending order, then by book title in ascending order.

	The result format is in the following example.

	Example:

	Input:

	library_books table:

	+---------+------------------------+------------------+----------+------------------+--------------+
	| book_id | title                  | author           | genre    | publication_year | total_copies |
	+---------+------------------------+------------------+----------+------------------+--------------+
	| 1       | The Great Gatsby       | F. Scott         | Fiction  | 1925             | 3            |
	| 2       | To Kill a Mockingbird  | Harper Lee       | Fiction  | 1960             | 3            |
	| 3       | 1984                   | George Orwell    | Dystopian| 1949             | 1            |
	| 4       | Pride and Prejudice    | Jane Austen      | Romance  | 1813             | 2            |
	| 5       | The Catcher in the Rye | J.D. Salinger    | Fiction  | 1951             | 1            |
	| 6       | Brave New World        | Aldous Huxley    | Dystopian| 1932             | 4            |
	+---------+------------------------+------------------+----------+------------------+--------------+
	borrowing_records table:

	+-----------+---------+---------------+-------------+-------------+
	| record_id | book_id | borrower_name | borrow_date | return_date |
	+-----------+---------+---------------+-------------+-------------+
	| 1         | 1       | Alice Smith   | 2024-01-15  | NULL        |
	| 2         | 1       | Bob Johnson   | 2024-01-20  | NULL        |
	| 3         | 2       | Carol White   | 2024-01-10  | 2024-01-25  |
	| 4         | 3       | David Brown   | 2024-02-01  | NULL        |
	| 5         | 4       | Emma Wilson   | 2024-01-05  | NULL        |
	| 6         | 5       | Frank Davis   | 2024-01-18  | 2024-02-10  |
	| 7         | 1       | Grace Miller  | 2024-02-05  | NULL        |
	| 8         | 6       | Henry Taylor  | 2024-01-12  | NULL        |
	| 9         | 2       | Ivan Clark    | 2024-02-12  | NULL        |
	| 10        | 2       | Jane Adams    | 2024-02-15  | NULL        |
	+-----------+---------+---------------+-------------+-------------+
	Output:

	+---------+------------------+---------------+-----------+------------------+-------------------+
	| book_id | title            | author        | genre     | publication_year | current_borrowers |
	+---------+------------------+---------------+-----------+------------------+-------------------+
	| 1       | The Great Gatsby | F. Scott      | Fiction   | 1925             | 3                 |
	| 3       | 1984             | George Orwell | Dystopian | 1949             | 1                 |
	+---------+------------------+---------------+-----------+------------------+-------------------+
	Explanation:

	The Great Gatsby (book_id = 1):
	Total copies: 3
	Currently borrowed by Alice Smith, Bob Johnson, and Grace Miller (3 borrowers)
	Available copies: 3 - 3 = 0
	Included because available_copies = 0
	1984 (book_id = 3):
	Total copies: 1
	Currently borrowed by David Brown (1 borrower)
	Available copies: 1 - 1 = 0
	Included because available_copies = 0
	Books not included:
	To Kill a Mockingbird (book_id = 2): Total copies = 3, current borrowers = 2, available = 1
	Pride and Prejudice (book_id = 4): Total copies = 2, current borrowers = 1, available = 1
	The Catcher in the Rye (book_id = 5): Total copies = 1, current borrowers = 0, available = 1
	Brave New World (book_id = 6): Total copies = 4, current borrowers = 1, available = 3
	Result ordering:
	The Great Gatsby appears first with 3 current borrowers
	1984 appears second with 1 current borrower
	Output table is ordered by current_borrowers in descending order, then by book_title in ascending order.
'''
from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# start timer to see execution time
start_timer()

#============ Data preparation===============
library_books_data = [
    (1, "The Great Gatsby", "F. Scott", "Fiction", 1925, 3),
    (2, "To Kill a Mockingbird", "Harper Lee", "Fiction", 1960, 3),
    (3, "1984", "George Orwell", "Dystopian", 1949, 1),
    (4, "Pride and Prejudice", "Jane Austen", "Romance", 1813, 2),
    (5, "The Catcher in the Rye", "J.D. Salinger", "Fiction", 1951, 1),
    (6, "Brave New World", "Aldous Huxley", "Dystopian", 1932, 4)
]

library_books_columns = ["book_id", "title", "author", "genre", "publication_year", "total_copies"]

# convert list to data frame
library_books_df = spark.createDataFrame(library_books_data,library_books_columns)

# Sample product data
borrowing_records_data = [
    (1, 1, "Alice Smith", "2024-01-15", None),
    (2, 1, "Bob Johnson", "2024-01-20", None),
    (3, 2, "Carol White", "2024-01-10", "2024-01-25"),
    (4, 3, "David Brown", "2024-02-01", None),
    (5, 4, "Emma Wilson", "2024-01-05", None),
    (6, 5, "Frank Davis", "2024-01-18", "2024-02-10"),
    (7, 1, "Grace Miller", "2024-02-05", None),
    (8, 6, "Henry Taylor", "2024-01-12", None),
    (9, 2, "Ivan Clark", "2024-02-12", None),
    (10, 2, "Jane Adams", "2024-02-15", None)
]

borrowing_records_columns = ["record_id", "book_id", "borrower_name", "borrow_date", "return_date"]

# convert list to data frame
borrowing_records_df = spark.createDataFrame(borrowing_records_data,borrowing_records_columns)

print()
print("==========Input Data=============")

library_books_df.show()
borrowing_records_df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)

# remove records those are returned
borrowing_records_df=borrowing_records_df.where(col("return_date").isNull())
# calculate the number of book
borrowing_records_df=borrowing_records_df.groupby("book_id").agg(count("*").alias("current_borrowers"))

df=(library_books_df.join(borrowing_records_df,["book_id"])
    .withColumn("remaining", col("total_copies")-col("current_borrowers"))
    .where(col("remaining")<=0)
    .drop(col("remaining"),col("total_copies"))
    .orderBy(desc("current_borrowers"),asc("title"))
)

df.show()

# # # # #### ================ Approach->2 : (SQL)
# library_books_df.createOrReplaceTempView("library_books")
# borrowing_records_df.createOrReplaceTempView("borrowing_records")
#
# sSQL="""
#
#     WITH borrow_book AS (
#         SELECT book_id,COUNT(*) AS current_borrowers FROM borrowing_records
#         WHERE return_date IS NULL GROUP BY book_id
#     ),remaining_book AS (
#         SELECT LB.*,BB.current_borrowers, (LB.total_copies-BB.current_borrowers) AS remaining
#         FROM library_books LB
#         INNER JOIN borrow_book BB ON LB.book_id=BB.book_id
#     )
#     SELECT book_id,title,author,genre,publication_year,current_borrowers
#     FROM remaining_book WHERE remaining=0 ORDER BY current_borrowers DESC, title ASC
#
# """
# df=spark.sql(sSQL)
# df.show()

## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()