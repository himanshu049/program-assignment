def most_occuring_letter(string):
    res, ans, temp = {}, "", -1
    for s in "".join(string.split()).lower():
        res[s] = res.get(s, 0) + 1
        if res[s] > temp:
            temp = res[s]
            ans = s
    return ans
string = "Not All Geeks Are Real Coders"
print(most_occuring_letter(string))

#join(string.split()).lower() : Removes space and converts every letter to lowercase.