'''
Given a pipe-delimited string containing multiple person records concatenated together,
write a solution to parse the string and extract each individual record with their complete information.

Input:

    "Rahul|BE|8|Bigdata|9876543210|Ramesh|BTech|6|Java|8765432109|Rajesh|BE|9|Linux|7890654321"

Output:

    Rahul|BE|8|Bigdata|9876543210
    Ramesh|BTech|6|Java|8765432109
    Rajesh|BE|9|Linux|7890654321


Explanation:

        1. Each person record contains exactly 5 fields: Name, Degree, Experience, Skill, Phone
        2. The output should separate each complete record on a new line
        3. Maintain all original fields in their original order
        4. Preserve the exact field values without modification
        5. Handle any number of complete records in the input string

'''

# input data
input_str = "Rahul|BE|8|Bigdata|9876543210|Ramesh|BTech|6|Java|8765432109|Rajesh|BE|9|Linux|7890654321"

print()
print("==========Input Data=============")

print(input_str)
print()
print("==========Expected output=============")

fields = input_str.split('|')

# Process records in chunks of 5
for i in range(0, len(fields), 5):
    oRow = fields[i:i + 5]
    print ('|'.join(oRow))

