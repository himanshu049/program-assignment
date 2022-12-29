# Given a list a = [1,2,3,4,5] and the positions by which use want to rotate the list in the right direction, return the rotated list
# example: rotateList(a, 1) => [5,1,2,3,4]
# example: rotateList(a, 2) => [4,5,1,2,3]
# example: rotateList(a, 3) => [3,4,5,1,2]
# example: rotateList(a, 7) => [2,3,4,5,1]
def rotateList(a, n):
    n = n%len(a)
    return a[-n::] + a[0:-n]
print(rotateList(a=[1,2,3,4,5], n=11))
#OUTPUT: [5, 1, 2, 3, 4]