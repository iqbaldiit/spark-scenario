'''
	Table: Samples

	+----------------+---------+
	| Column Name    | Type    |
	+----------------+---------+
	| sample_id      | int     |
	| dna_sequence   | varchar |
	| species        | varchar |
	+----------------+---------+
	sample_id is the unique key for this table.
	Each row contains a DNA sequence represented as a string of characters (A, T, G, C) and the species it was collected from.
	Biologists are studying basic patterns in DNA sequences. Write a solution to identify sample_id with the following patterns:

	Sequences that start with ATG (a common start codon)
	Sequences that end with either TAA, TAG, or TGA (stop codons)
	Sequences containing the motif ATAT (a simple repeated pattern)
	Sequences that have at least 3 consecutive G (like GGG or GGGG)
	Return the result table ordered by sample_id in ascending order.

	The result format is in the following example.



	Example:

	Input:

	Samples table:

	+-----------+------------------+-----------+
	| sample_id | dna_sequence     | species   |
	+-----------+------------------+-----------+
	| 1         | ATGCTAGCTAGCTAA  | Human     |
	| 2         | GGGTCAATCATC     | Human     |
	| 3         | ATATATCGTAGCTA   | Human     |
	| 4         | ATGGGGTCATCATAA  | Mouse     |
	| 5         | TCAGTCAGTCAG     | Mouse     |
	| 6         | ATATCGCGCTAG     | Zebrafish |
	| 7         | CGTATGCGTCGTA    | Zebrafish |
	+-----------+------------------+-----------+
	Output:

	+-----------+------------------+-------------+-------------+------------+------------+------------+
	| sample_id | dna_sequence     | species     | has_start   | has_stop   | has_atat   | has_ggg    |
	+-----------+------------------+-------------+-------------+------------+------------+------------+
	| 1         | ATGCTAGCTAGCTAA  | Human       | 1           | 1          | 0          | 0          |
	| 2         | GGGTCAATCATC     | Human       | 0           | 0          | 0          | 1          |
	| 3         | ATATATCGTAGCTA   | Human       | 0           | 0          | 1          | 0          |
	| 4         | ATGGGGTCATCATAA  | Mouse       | 1           | 1          | 0          | 1          |
	| 5         | TCAGTCAGTCAG     | Mouse       | 0           | 0          | 0          | 0          |
	| 6         | ATATCGCGCTAG     | Zebrafish   | 0           | 1          | 1          | 0          |
	| 7         | CGTATGCGTCGTA    | Zebrafish   | 0           | 0          | 0          | 0          |
	+-----------+------------------+-------------+-------------+------------+------------+------------+
	Explanation:

	Sample 1 (ATGCTAGCTAGCTAA):
	Starts with ATG (has_start = 1)
	Ends with TAA (has_stop = 1)
	Does not contain ATAT (has_atat = 0)
	Does not contain at least 3 consecutive 'G's (has_ggg = 0)
	Sample 2 (GGGTCAATCATC):
	Does not start with ATG (has_start = 0)
	Does not end with TAA, TAG, or TGA (has_stop = 0)
	Does not contain ATAT (has_atat = 0)
	Contains GGG (has_ggg = 1)
	Sample 3 (ATATATCGTAGCTA):
	Does not start with ATG (has_start = 0)
	Does not end with TAA, TAG, or TGA (has_stop = 0)
	Contains ATAT (has_atat = 1)
	Does not contain at least 3 consecutive 'G's (has_ggg = 0)
	Sample 4 (ATGGGGTCATCATAA):
	Starts with ATG (has_start = 1)
	Ends with TAA (has_stop = 1)
	Does not contain ATAT (has_atat = 0)
	Contains GGGG (has_ggg = 1)
	Sample 5 (TCAGTCAGTCAG):
	Does not match any patterns (all fields = 0)
	Sample 6 (ATATCGCGCTAG):
	Does not start with ATG (has_start = 0)
	Ends with TAG (has_stop = 1)
	Starts with ATAT (has_atat = 1)
	Does not contain at least 3 consecutive 'G's (has_ggg = 0)
	Sample 7 (CGTATGCGTCGTA):
	Does not start with ATG (has_start = 0)
	Does not end with TAA, "TAG", or "TGA" (has_stop = 0)
	Does not contain ATAT (has_atat = 0)
	Does not contain at least 3 consecutive 'G's (has_ggg = 0)
	Note:

	The result is ordered by sample_id in ascending order
	For each pattern, 1 indicates the pattern is present and 0 indicates it is not present

'''
from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# start timer to see execution time
start_timer()

#============ Data preparation===============
data = [
    (1, 'ATGCTAGCTAGCTAA', 'Human'),
    (2, 'GGGTCAATCATC', 'Human'),
    (3, 'ATATATCGTAGCTA', 'Human'),
    (4, 'ATGGGGTCATCATAA', 'Mouse'),
    (5, 'TCAGTCAGTCAG', 'Mouse'),
    (6, 'ATATCGCGCTAG', 'Zebrafish'),
    (7, 'CGTATGCGTCGTA', 'Zebrafish')
]

columns = ["sample_id", "dna_sequence", "species"]

# convert list to data frame
df = spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # #### ================ Approach->1 : (DSL)

df= (df.withColumn('has_start', when(col('dna_sequence').startswith('ATG'),1).otherwise(0))
     .withColumn('has_stop', when(col('dna_sequence').startswith('TAA')
                                  | col('dna_sequence').startswith('TAG')
                                  | col('dna_sequence').startswith('TGA'),1).otherwise(0))
     .withColumn('has_atat', when(col('dna_sequence').contains('ATAT'),1).otherwise(0))
     .withColumn('has_ggg', when(col('dna_sequence').contains('GGG'),1).otherwise(0))
     .orderBy('sample_id')
)
df.show()

# # # # #### ================ Approach->2 : (SQL)
# df.createOrReplaceTempView("Samples")
#
# sSQL="""
#
#     SELECT sample_id,dna_sequence,species
#     ,CASE WHEN dna_sequence LIKE 'ATG%' THEN 1 ELSE 0 END has_start
#     ,CASE WHEN dna_sequence LIKE '%TAA' OR  dna_sequence LIKE '%TAG' OR dna_sequence LIKE '%TGA'
#         THEN 1 ELSE 0 END has_stop
#     ,CASE WHEN dna_sequence LIKE '%ATAT%' THEN 1 ELSE 0 END has_atat
#     ,CASE WHEN dna_sequence LIKE '%GGG%' THEN 1 ELSE 0 END has_ggg
#     FROM Samples ORDER BY sample_id ASC
#
# """
# df=spark.sql(sSQL)
# df.show()

## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()



