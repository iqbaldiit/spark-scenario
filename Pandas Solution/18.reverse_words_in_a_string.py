'''
Write a PySpark solution to reverse each word in a string while preserving the
original word order and whitespace.
Given a DataFrame with a single string column, you should transform it such that each word
in the string is reversed, while maintaining the original structure of the string.

Input:
    +------------------+
    |              word|
    +------------------+
    |The Social Dilemma|
    +------------------+

Output:

    +------------------+
    |      reverse word|
    +------------------+
    |ehT laicoS ammeliD|
    +------------------+

Solution Explanation: The problem stated that, only the words of the sentence will be reversed not whole sentence

Approach:
    1. Split the sentence to array
    2. reverse each element of the array
    3. concat the array
    4. show()
'''

import pandas as pd

# dataframe
df = pd.DataFrame({"word": ["The Social Dilemma"]})

df["reverse_word"] = (
  df['word']
  .str.split(' ')
  .apply(lambda x: [w[::-1] for w in x])
  .str.join(' ')
)

print(df['reverse_word'])