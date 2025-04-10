input="abcdabcd"
s=sorted(set(input))
print(s)
result=""
for c in s:
    count=0
    for i in input:
        if c==i:
            count=count+1
    result=result+c+str(count)
print(result)